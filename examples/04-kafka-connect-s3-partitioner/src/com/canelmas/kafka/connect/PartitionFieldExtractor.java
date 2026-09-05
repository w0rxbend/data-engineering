/*
 * Copyright (C) 2020 Can Elmas <canelm@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.canelmas.kafka.connect;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns the JavaScript Object Notation (JSON) body of a Kafka record into the
 * first half of an object storage path, for example {@code appId=shop-web/country=DE}.
 *
 * <p>This class is deliberately free of any Kafka Connect configuration or
 * Amazon Simple Storage Service (S3) client code: it takes a list of field
 * names and returns a string. That makes the path rules unit-testable without
 * a broker, a connector runtime or a bucket.
 *
 * <p>A field name may address a nested value with dots, so {@code shipping.country}
 * reads {@code {"shipping": {"country": "DE"}}}. A field that is missing, null
 * or not a plain value becomes {@link #UNKNOWN_VALUE} rather than failing the
 * task, because a sink that dies on one malformed record blocks the whole topic.
 */
public final class PartitionFieldExtractor {

  /** Placeholder written into the path when a field cannot be read from a record. */
  public static final String UNKNOWN_VALUE = "unknown";

  /** Separator between one path segment and the next, as used by Hive-style layouts. */
  static final String PATH_DELIMITER = "/";

  /** Separator between a field name and its value in {@code name=value} segments. */
  private static final String NAME_VALUE_DELIMITER = "=";

  private static final String NESTED_FIELD_SEPARATOR = "\\.";

  private static final char[] HEX = "0123456789ABCDEF".toCharArray();

  private static final Logger log = LoggerFactory.getLogger(PartitionFieldExtractor.class);

  /** One entry per configured field, in configuration order, already split on dots. */
  private final List<FieldPath> fieldPaths;

  private final boolean includeFieldNamesInPath;

  /**
   * @param fieldNames record fields to partition by, in the order they should appear in the path
   * @param includeFieldNamesInPath {@code true} writes {@code country=DE}, {@code false} writes {@code DE}
   */
  public PartitionFieldExtractor(final List<String> fieldNames, final boolean includeFieldNamesInPath) {
    Objects.requireNonNull(fieldNames, "fieldNames");
    final List<FieldPath> paths = new ArrayList<>(fieldNames.size());
    for (final String fieldName : fieldNames) {
      paths.add(FieldPath.parse(fieldName));
    }
    this.fieldPaths = Collections.unmodifiableList(paths);
    this.includeFieldNamesInPath = includeFieldNamesInPath;
  }

  /** Builds the field part of the storage path for one record. */
  public String extract(final ConnectRecord<?> record) {
    return extractFromJson(String.valueOf(record.value()));
  }

  /** Builds the field part of the storage path from a raw JSON document. */
  public String extractFromJson(final String json) {
    final JsonElement root = parse(json);
    final StringJoiner path = new StringJoiner(PATH_DELIMITER);
    for (final FieldPath fieldPath : fieldPaths) {
      path.add(segmentFor(fieldPath, root));
    }
    return path.toString();
  }

  private String segmentFor(final FieldPath fieldPath, final JsonElement root) {
    final String value = encodePathSegment(fieldPath.readValueFrom(root));
    return includeFieldNamesInPath ? fieldPath.name() + NAME_VALUE_DELIMITER + value : value;
  }

  /**
   * Percent-encodes bytes that would change the object-key hierarchy.
   *
   * <p>Partition values are data, not path syntax. Without this boundary a value such as
   * {@code retail/eu} silently creates an extra directory and stops matching the advertised
   * Hive partition schema. UTF-8 byte encoding also makes non-ASCII values deterministic across
   * Connect workers regardless of their default charset.
   */
  static String encodePathSegment(final String raw) {
    if (raw.isEmpty()) {
      return UNKNOWN_VALUE;
    }
    final StringBuilder encoded = new StringBuilder(raw.length());
    for (final byte value : raw.getBytes(StandardCharsets.UTF_8)) {
      final int unsigned = value & 0xff;
      if (isUnreserved(unsigned)) {
        encoded.append((char) unsigned);
      } else {
        encoded.append('%').append(HEX[unsigned >>> 4]).append(HEX[unsigned & 0x0f]);
      }
    }
    return encoded.toString();
  }

  private static boolean isUnreserved(final int value) {
    return (value >= 'a' && value <= 'z')
        || (value >= 'A' && value <= 'Z')
        || (value >= '0' && value <= '9')
        || value == '-'
        || value == '_'
        || value == '.';
  }

  private static JsonElement parse(final String json) {
    try {
      return JsonParser.parseString(json);
    } catch (final RuntimeException failure) {
      log.warn("Record value is not valid JSON, partitioning it as '{}'", UNKNOWN_VALUE, failure);
      return JsonNull.INSTANCE;
    }
  }

  /** A configured field name together with the parsed steps needed to reach it. */
  private static final class FieldPath {

    private final String name;
    private final List<String> steps;

    private FieldPath(final String name, final List<String> steps) {
      this.name = name;
      this.steps = steps;
    }

    static FieldPath parse(final String fieldName) {
      final String trimmed = fieldName.trim();
      if (trimmed.isEmpty()) {
        throw new IllegalArgumentException("partition field names must not be blank");
      }
      return new FieldPath(trimmed, List.of(trimmed.split(NESTED_FIELD_SEPARATOR)));
    }

    String name() {
      return name;
    }

    /** Walks the dotted path and renders the value, or {@link #UNKNOWN_VALUE} if it is unreachable. */
    String readValueFrom(final JsonElement root) {
      JsonElement current = root;
      for (final String step : steps) {
        if (current == null || !current.isJsonObject()) {
          log.warn("Field '{}' is not present in the record, partitioning it as '{}'", name, UNKNOWN_VALUE);
          return UNKNOWN_VALUE;
        }
        final JsonObject object = current.getAsJsonObject();
        current = object.get(step);
      }
      return render(current);
    }

    private String render(final JsonElement element) {
      if (element == null || element.isJsonNull() || !element.isJsonPrimitive()) {
        log.warn("Field '{}' has no simple value, partitioning it as '{}'", name, UNKNOWN_VALUE);
        return UNKNOWN_VALUE;
      }
      return element.getAsString();
    }
  }
}

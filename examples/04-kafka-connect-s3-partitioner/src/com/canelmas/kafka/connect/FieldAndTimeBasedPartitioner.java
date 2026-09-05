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

import static io.confluent.connect.storage.partitioner.PartitionerConfig.PARTITION_FIELD_NAME_CONFIG;

import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTimeZone;

/**
 * A Kafka Connect partitioner that lays records out by record <em>field</em> first
 * and by record <em>time</em> second.
 *
 * <p>A partitioner decides the directory an object storage sink writes a record into.
 * The stock {@code TimeBasedPartitioner} shipped by Confluent only uses time, which
 * produces {@code year=2020/month=11/day=30}. This subclass prefixes that with values
 * read out of the record body, producing
 * {@code appId=shop-web/country=DE/year=2020/month=11/day=30}.
 *
 * <p>That layout matters because query engines such as Apache Hive, Trino, Amazon
 * Athena and Apache Spark read those {@code name=value} directory names as table
 * columns ("Hive-style partitions"). A query filtered on one country then reads one
 * directory instead of the whole bucket.
 *
 * <p>Everything the stock time partitioner understands still applies: {@code path.format},
 * {@code partition.duration.ms}, {@code locale}, {@code timezone} and the timestamp
 * extractor are all inherited unchanged.
 *
 * <p>Original implementation by Can Elmas, licensed under the Apache License 2.0.
 *
 * @param <T> the schema representation used by the sink, supplied by Kafka Connect
 */
public final class FieldAndTimeBasedPartitioner<T> extends TimeBasedPartitioner<T> {

  /** Connector setting choosing between {@code country=DE} and a bare {@code DE} segment. */
  public static final String PARTITION_FIELD_FORMAT_PATH_CONFIG = "partition.field.format.path";

  public static final String PARTITION_FIELD_FORMAT_PATH_DOC =
      "Whether directory labels should be included when partitioning for custom fields, "
          + "that is 'orgId=XXXX/appId=ZZZZ' rather than 'XXXX/ZZZZ'.";

  public static final String PARTITION_FIELD_FORMAT_PATH_DISPLAY = "Partition Field Format Path";

  public static final boolean PARTITION_FIELD_FORMAT_PATH_DEFAULT = true;

  private PartitionFieldExtractor fieldExtractor;

  /**
   * Called once by Kafka Connect after the connector configuration has been parsed.
   *
   * <p>Kafka Connect hands over every setting as a string, including the boolean one,
   * so {@code partition.field.format.path} is parsed here rather than cast.
   */
  @Override
  protected void init(
      final long partitionDurationMs,
      final String pathFormat,
      final Locale locale,
      final DateTimeZone timeZone,
      final Map<String, Object> config) {

    super.init(partitionDurationMs, pathFormat, locale, timeZone, config);
    this.fieldExtractor = new PartitionFieldExtractor(fieldNames(config), formatPath(config));
  }

  /** Builds the storage path for a record, using the record's own timestamp. */
  @Override
  public String encodePartition(final SinkRecord sinkRecord) {
    return join(fieldExtractor.extract(sinkRecord), super.encodePartition(sinkRecord));
  }

  /** Builds the storage path for a record, using wall-clock time as the fallback timestamp. */
  @Override
  public String encodePartition(final SinkRecord sinkRecord, final long nowInMillis) {
    return join(fieldExtractor.extract(sinkRecord), super.encodePartition(sinkRecord, nowInMillis));
  }

  private String join(final String fieldPartitions, final String timePartitions) {
    return String.join(this.delim, fieldPartitions, timePartitions);
  }

  @SuppressWarnings("unchecked")
  private static List<String> fieldNames(final Map<String, Object> config) {
    final Object configured = config.get(PARTITION_FIELD_NAME_CONFIG);
    if (!(configured instanceof List)) {
      throw new IllegalArgumentException(
          "'" + PARTITION_FIELD_NAME_CONFIG + "' must be set to a comma separated list of record fields");
    }
    final List<String> fieldNames = (List<String>) configured;
    if (fieldNames.isEmpty()) {
      throw new IllegalArgumentException("'" + PARTITION_FIELD_NAME_CONFIG + "' must name at least one field");
    }
    return fieldNames;
  }

  private static boolean formatPath(final Map<String, Object> config) {
    final Object configured = config.get(PARTITION_FIELD_FORMAT_PATH_CONFIG);
    if (configured == null) {
      return PARTITION_FIELD_FORMAT_PATH_DEFAULT;
    }
    return Boolean.parseBoolean(String.valueOf(configured));
  }
}

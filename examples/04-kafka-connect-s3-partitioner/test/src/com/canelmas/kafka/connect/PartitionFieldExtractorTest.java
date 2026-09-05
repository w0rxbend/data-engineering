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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Field-to-path rules, exercised without a broker, a connector runtime or a bucket. */
class PartitionFieldExtractorTest {

  private static final String ORDER =
      "{\"orderId\":\"o-1\",\"channel\":\"web\",\"shipping\":{\"country\":\"DE\"},\"totalCents\":4999}";

  @Test
  @DisplayName("writes one name=value segment per configured field, in configuration order")
  void labelsEverySegment() {
    final PartitionFieldExtractor extractor = new PartitionFieldExtractor(List.of("channel", "orderId"), true);

    assertEquals("channel=web/orderId=o-1", extractor.extractFromJson(ORDER));
  }

  @Test
  @DisplayName("writes bare values when field labels are switched off")
  void omitsLabelsWhenAsked() {
    final PartitionFieldExtractor extractor = new PartitionFieldExtractor(List.of("channel", "orderId"), false);

    assertEquals("web/o-1", extractor.extractFromJson(ORDER));
  }

  @Test
  @DisplayName("reads a nested field through a dotted name")
  void readsNestedField() {
    final PartitionFieldExtractor extractor = new PartitionFieldExtractor(List.of("shipping.country"), true);

    assertEquals("shipping.country=DE", extractor.extractFromJson(ORDER));
  }

  @Test
  @DisplayName("renders a numeric field as its plain text value")
  void readsNumericField() {
    final PartitionFieldExtractor extractor = new PartitionFieldExtractor(List.of("totalCents"), true);

    assertEquals("totalCents=4999", extractor.extractFromJson(ORDER));
  }

  @Test
  @DisplayName("encodes path syntax and Unicode inside field values")
  void encodesUnsafePathValues() {
    final PartitionFieldExtractor extractor = new PartitionFieldExtractor(List.of("channel"), true);

    assertEquals("channel=retail%2Feu%3Dvip%20%F0%9F%9A%80", extractor.extractFromJson("{\"channel\":\"retail/eu=vip 🚀\"}"));
  }

  @Test
  @DisplayName("uses the unknown partition for an empty field value")
  void treatsEmptyValuesAsUnknown() {
    final PartitionFieldExtractor extractor = new PartitionFieldExtractor(List.of("channel"), true);

    assertEquals("channel=unknown", extractor.extractFromJson("{\"channel\":\"\"}"));
  }

  @Test
  @DisplayName("falls back to 'unknown' instead of failing the sink task")
  void survivesUnusableRecords() {
    final PartitionFieldExtractor extractor = new PartitionFieldExtractor(List.of("channel"), true);

    assertEquals("channel=unknown", extractor.extractFromJson("{\"orderId\":\"o-1\"}"), "field absent");
    assertEquals("channel=unknown", extractor.extractFromJson("{\"channel\":null}"), "field is null");
    assertEquals("channel=unknown", extractor.extractFromJson("{\"channel\":{\"id\":1}}"), "field is an object");
    assertEquals("channel=unknown", extractor.extractFromJson("not json at all"), "record is not JSON");
  }

  @Test
  @DisplayName("falls back to 'unknown' when a dotted path runs into a non-object")
  void survivesBrokenNestedPath() {
    final PartitionFieldExtractor extractor = new PartitionFieldExtractor(List.of("shipping.country"), true);

    assertEquals("shipping.country=unknown", extractor.extractFromJson("{\"shipping\":\"DE\"}"));
  }

  @Test
  @DisplayName("rejects a blank field name at construction time rather than per record")
  void rejectsBlankFieldName() {
    assertThrows(IllegalArgumentException.class, () -> new PartitionFieldExtractor(List.of("  "), true));
  }

  @Test
  @DisplayName("produces an empty prefix when no field is configured")
  void producesEmptyPrefixWithoutFields() {
    assertEquals("", new PartitionFieldExtractor(List.of(), true).extractFromJson(ORDER));
  }
}

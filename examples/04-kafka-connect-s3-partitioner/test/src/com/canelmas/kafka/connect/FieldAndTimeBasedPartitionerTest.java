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

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * End-to-end path building, still without a broker or a bucket: the partitioner is
 * configured by hand exactly the way a Kafka Connect worker would configure it.
 */
class FieldAndTimeBasedPartitionerTest {

  /** 2020-11-30T23:30:00Z. In Berlin (UTC+1 in November) that is already 2020-12-01. */
  private static final long LATE_NOVEMBER_UTC = 1606779000000L;

  private static final String ORDER = "{\"orderId\":\"o-1\",\"channel\":\"web\",\"shipping\":{\"country\":\"DE\"}}";

  @Test
  @DisplayName("puts the field partitions in front of the time partitions")
  void combinesFieldsAndTime() {
    final FieldAndTimeBasedPartitioner<String> partitioner = configured("UTC", "channel,shipping.country");

    assertEquals(
        "channel=web/shipping.country=DE/year=2020/month=11/day=30",
        partitioner.encodePartition(orderAt(LATE_NOVEMBER_UTC)));
  }

  @Test
  @DisplayName("uses the configured time zone when bucketing the record timestamp")
  void honoursConfiguredTimeZone() {
    final FieldAndTimeBasedPartitioner<String> berlin = configured("Europe/Berlin", "channel");

    assertEquals("channel=web/year=2020/month=12/day=01", berlin.encodePartition(orderAt(LATE_NOVEMBER_UTC)));
  }

  @Test
  @DisplayName("gives every record of the same day and channel the same path")
  void groupsRecordsOfTheSameDay() {
    final FieldAndTimeBasedPartitioner<String> partitioner = configured("UTC", "channel");
    final String morning = partitioner.encodePartition(orderAt(LATE_NOVEMBER_UTC - 20 * 60 * 60 * 1000L));

    assertEquals(morning, partitioner.encodePartition(orderAt(LATE_NOVEMBER_UTC)));
  }

  @Test
  @DisplayName("still writes a path when the field is missing from the record")
  void keepsWritingWhenAFieldIsMissing() {
    final FieldAndTimeBasedPartitioner<String> partitioner = configured("UTC", "channel");
    final SinkRecord withoutChannel = recordWithValue("{\"orderId\":\"o-2\"}", LATE_NOVEMBER_UTC);

    assertEquals("channel=unknown/year=2020/month=11/day=30", partitioner.encodePartition(withoutChannel));
  }

  @Test
  @DisplayName("rejects a configuration that names no partition field")
  void rejectsMissingFieldNames() {
    assertThrows(IllegalArgumentException.class, () -> configured("UTC", ""));
  }

  @Test
  @DisplayName("rejects a misspelled path-format boolean instead of silently changing the layout")
  void rejectsInvalidPathFormatBoolean() {
    final FieldAndTimeBasedPartitioner<String> partitioner = new FieldAndTimeBasedPartitioner<>();
    final Map<String, Object> settings = connectorSettings("UTC", "channel");
    settings.put(FieldAndTimeBasedPartitioner.PARTITION_FIELD_FORMAT_PATH_CONFIG, "treu");

    assertThrows(IllegalArgumentException.class, () -> partitioner.configure(settings));
  }

  private static FieldAndTimeBasedPartitioner<String> configured(final String timeZone, final String fieldNames) {
    final FieldAndTimeBasedPartitioner<String> partitioner = new FieldAndTimeBasedPartitioner<>();
    partitioner.configure(connectorSettings(timeZone, fieldNames));
    return partitioner;
  }

  /** The subset of connector settings a partitioner reads, shaped the way the worker passes them. */
  private static Map<String, Object> connectorSettings(final String timeZone, final String fieldNames) {
    final Map<String, Object> settings = new HashMap<>();
    settings.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, StorageCommonConfig.DIRECTORY_DELIM_DEFAULT);
    settings.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, 86400000L);
    settings.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'year'=YYYY/'month'=MM/'day'=dd");
    settings.put(PartitionerConfig.LOCALE_CONFIG, "en-US");
    settings.put(PartitionerConfig.TIMEZONE_CONFIG, timeZone);
    settings.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record");
    settings.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, splitFieldNames(fieldNames));
    return settings;
  }

  private static List<String> splitFieldNames(final String fieldNames) {
    return fieldNames.isEmpty() ? List.of() : List.of(fieldNames.split(","));
  }

  private static SinkRecord orderAt(final long timestampMs) {
    return recordWithValue(ORDER, timestampMs);
  }

  private static SinkRecord recordWithValue(final String value, final long timestampMs) {
    return new SinkRecord(
        "orders",
        0,
        Schema.STRING_SCHEMA,
        "o-1",
        Schema.STRING_SCHEMA,
        value,
        0L,
        timestampMs,
        org.apache.kafka.common.record.TimestampType.CREATE_TIME);
  }
}

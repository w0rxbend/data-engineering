# 04 - A custom Kafka Connect partitioner for object storage

This example builds a **plugin** for Kafka Connect: a small Java library that decides
*which folder* every record ends up in when it is archived to object storage. Instead of
the usual date-only layout it writes values taken from the record itself into the path
first, so an archive of online-shop orders is laid out as
`channel=web/shipping.country=DE/year=YYYY/month=MM/day=DD/` rather than only
`year=YYYY/month=MM/day=DD/`.

The example is a migration of [Can Elmas' `kafka-connect-field-and-time-partitioner`][origin]
into this repository's Mill build, with the original Apache License 2.0 and attribution kept
intact. It adds unit tests, an assembled plugin jar and a complete local stack you can run.

[origin]: https://github.com/canelmas/kafka-connect-field-and-time-partitioner

## Vocabulary for newcomers

Some words are used constantly below, so here is what each one means.

- **Apache Kafka** is a log of messages ("records"). Producers append to a **topic**,
  consumers read from it.
- **Kafka Connect** is a ready-made worker process that copies data between Kafka and other
  systems, so you do not have to write a consumer yourself. A **sink connector** copies data
  *out of* Kafka into something else; a **source connector** copies data *into* Kafka.
- A **converter** turns a record between the bytes on the Kafka topic and the in-memory shape
  Connect works with. `StringConverter` treats the bytes as text; `AvroConverter` and
  `JsonConverter` understand a schema. It answers the question *"what does this record mean?"*
- A **partitioner** answers a different question: *"where should this record be stored?"* It
  returns a path fragment, and the sink writes the record into an object under that path. The
  partitioner in this example is the custom part; the converter and the sink itself are stock
  Confluent components.
- **Amazon Simple Storage Service (S3)** is object storage: a flat store of keys and blobs
  where a key like `raw/orders/year=YYYY/…/part.json` merely *looks* like a folder path.
  **MinIO** is an open-source server that speaks the same protocol, which is why the stack
  below needs no cloud account.
- **Hive-style partitioning** is the convention of encoding a column into a directory name as
  `name=value`. Query engines such as Apache Hive, Trino, Amazon Athena and Apache Spark read
  those directory names as real table columns. A query filtered on `country = 'DE'` then opens
  one directory instead of scanning the whole bucket - the reason this partitioner exists.

## The use case

The shop publishes every order to the `orders` topic. Analysts want a queryable archive in
object storage, and almost every question they ask is scoped to a sales channel, a shipping
country and a day: *"what did mobile customers in Poland spend yesterday?"*

With the stock time-based partitioner, answering that means reading every order of that day
from every channel and country. With this partitioner, channel and country are directories,
so the engine skips everything else before reading a single byte.

## How it works

| File | What it does |
| --- | --- |
| `src/com/canelmas/kafka/connect/PartitionFieldExtractor.java` | The pure logic: given a list of field names and a JSON record body, it returns the field half of the path, for example `channel=web/shipping.country=DE`. |
| `src/com/canelmas/kafka/connect/FieldAndTimeBasedPartitioner.java` | The Kafka Connect plugin class. It extends Confluent's `TimeBasedPartitioner` and glues the field half in front of the time half. |
| `test/src/com/canelmas/kafka/connect/PartitionFieldExtractorTest.java` | Field-to-path rules: labels on and off, nested fields, numbers, records that cannot be read. |
| `test/src/com/canelmas/kafka/connect/FieldAndTimeBasedPartitionerTest.java` | The whole partitioner, configured by hand exactly as a worker would configure it, including time-zone handling. |
| `package.mill` | The Mill build. Produces both a plain jar and an assembled plugin jar. |
| `docker/docker-compose.yml` | Kafka, Kafka Connect and MinIO, wired together. |
| `docker/Dockerfile.connect` | The Connect worker image with the Confluent S3 sink plugin baked in. |

Three decisions in that code are worth explaining.

**The path logic lives in its own class.** `PartitionFieldExtractor` knows nothing about
Kafka Connect configuration, brokers or buckets - it takes a list of field names and a piece
of JSON and returns a string. That is what lets the tests run in milliseconds with no Docker.
`FieldAndTimeBasedPartitioner` is the thin layer that reads the worker's configuration and
calls it.

**A record that cannot be read does not kill the connector.** If a configured field is
missing, null, or holds an object rather than a plain value, the extractor writes `unknown`
into the path and logs a warning. A sink task that threw instead would stop, and a stopped
sink task blocks every later record on the topic. Configuration mistakes are treated the
opposite way: an empty `partition.field.name` fails immediately at start-up, where a human is
watching, rather than silently producing paths with no field in them.

**Field values cannot inject path structure.** Values are UTF-8 percent-encoded before they become
object-key segments: `retail/eu` becomes `retail%2Feu`, and an empty string becomes `unknown`.
Without that boundary, ordinary source data containing `/`, `=` or whitespace could create extra
directories and break the Hive partition schema. Configuration field names are trusted schema
input; record values are not.

**Confluent's libraries are compiled against but not bundled.** In `package.mill` they sit in
`compileMvnDeps`, not `mvnDeps`. The Connect worker already has those classes on its plugin
classpath, and shipping a second copy inside the plugin jar is a classic cause of
`NoSuchMethodError` at run time. Google's Gson is the one library that *is* bundled, so the
assembled jar also works beside a connector that does not ship it.

### Configuration reference

| Setting | Meaning |
| --- | --- |
| `partitioner.class` | Must be `com.canelmas.kafka.connect.FieldAndTimeBasedPartitioner`. |
| `partition.field.name` | Comma-separated record fields to partition by, in path order. A dot addresses a nested field, so `shipping.country` reads `{"shipping":{"country":"DE"}}`. |
| `partition.field.format.path` | `true` writes `country=DE`, `false` writes a bare `DE`. Defaults to `true`; any other value fails connector startup. |
| `path.format`, `partition.duration.ms`, `locale`, `timezone`, `timestamp.extractor` | Inherited unchanged from Confluent's `TimeBasedPartitioner` and used for the time half of the path. |

## Run it

Every command is run from the repository root.

**1. Build the plugin jar.** The Compose file mounts it into the worker, so this has to
happen before `up`.

```bash
./mill examples.04-kafka-connect-s3-partitioner.assembly
```

**2. Start Kafka, Kafka Connect and MinIO.** The first run also builds the Connect image,
which downloads the S3 sink plugin and takes a couple of minutes.

```bash
docker compose -f examples/04-kafka-connect-s3-partitioner/docker/docker-compose.yml up -d --wait
```

`--wait` covers the long-running health checks. MinIO bucket creation is a one-shot job, so verify
it explicitly before registering the connector:

```bash
docker compose -f examples/04-kafka-connect-s3-partitioner/docker/docker-compose.yml \
  ps --all minio-setup
```

Its state must be `Exited (0)`.

Host ports, all inside this example's assigned 10400-10499 range:

| Address | What it is |
| --- | --- |
| `localhost:10492` | Kafka bootstrap server |
| <http://localhost:10483> | Kafka Connect REST interface |
| <http://localhost:10490> | MinIO S3 endpoint |
| <http://localhost:10491> | MinIO web console, user `de04accesskey`, password `de04secretkey` |

**3. Register the sink connector.** This is the command that puts the custom partitioner to
work; `partitioner.class` and `partition.field.name` are the two lines that matter.

```bash
curl -sS -X POST http://localhost:10483/connectors \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "orders-to-minio",
    "config": {
      "connector.class": "io.confluent.connect.s3.S3SinkConnector",
      "tasks.max": "1",
      "topics": "orders",
      "s3.bucket.name": "orders-lake",
      "s3.region": "us-east-1",
      "store.url": "http://minio:9000",
      "topics.dir": "raw",
      "flush.size": "2",
      "storage.class": "io.confluent.connect.s3.storage.S3Storage",
      "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.storage.StringConverter",
      "schema.compatibility": "NONE",
      "partitioner.class": "com.canelmas.kafka.connect.FieldAndTimeBasedPartitioner",
      "partition.field.name": "channel,shipping.country",
      "partition.field.format.path": "true",
      "partition.duration.ms": "86400000",
      "path.format": "'"'"'year'"'"'=YYYY/'"'"'month'"'"'=MM/'"'"'day'"'"'=dd",
      "locale": "en-US",
      "timezone": "UTC",
      "timestamp.extractor": "Record"
    }
  }'
```

Check that it started:

```bash
curl -sS http://localhost:10483/connectors/orders-to-minio/status
```

Expect `"state":"RUNNING"` for both the connector and its single task.

**4. Publish four orders.** `flush.size` is 2, so each pair of orders in the same partition
becomes one object straight away.

```bash
docker exec -i de-04-kafka kafka-console-producer --bootstrap-server kafka:9092 --topic orders <<'EOF'
{"orderId":"o-1","channel":"web","shipping":{"country":"DE"},"totalCents":4999}
{"orderId":"o-2","channel":"web","shipping":{"country":"DE"},"totalCents":1200}
{"orderId":"o-3","channel":"mobile","shipping":{"country":"PL"},"totalCents":800}
{"orderId":"o-4","channel":"mobile","shipping":{"country":"PL"},"totalCents":300}
EOF
```

**5. Look at the paths.** Give the worker a few seconds, then list the bucket:

```bash
docker run --rm --network de-04-partitioner --entrypoint sh \
  minio/mc:RELEASE.2024-09-16T17-43-14Z \
  -c "mc alias set local http://minio:9000 de04accesskey de04secretkey >/dev/null && mc ls -r local/orders-lake"
```

The two objects carry the channel and the country in their key, ahead of the date:

```
raw/orders/channel=mobile/shipping.country=PL/year=YYYY/month=MM/day=DD/orders+0+0000000002.json
raw/orders/channel=web/shipping.country=DE/year=YYYY/month=MM/day=DD/orders+0+0000000000.json
```

`YYYY/MM/DD` is today's date in UTC, because `timestamp.extractor` is `Record` and the
console producer stamps each record with the current time.

The same listing is visible in the MinIO console at <http://localhost:10491>.

**Run the unit tests** at any time - they need none of the above:

```bash
./mill examples.04-kafka-connect-s3-partitioner.test
```

## What to try next

- **Drop the labels.** Delete the connector, set `"partition.field.format.path": "false"` and
  register it again. Paths become `mobile/PL/year=…`, which is smaller but no longer readable
  as table columns by a query engine.
- **Change the time granularity.** Set `"partition.duration.ms": "3600000"` and
  `"path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH"` for hourly folders. More
  folders mean more, smaller objects - the trade-off every data lake has to tune.
- **Send a record with a missing field**, for example `{"orderId":"o-5","channel":"web"}`.
  It lands under `shipping.country=unknown` instead of stopping the connector; the reasoning
  is in "How it works" above.
- **Change the time zone** to `Europe/Berlin` and publish an order timestamped just before
  midnight UTC. It moves to the next day's folder, which is exactly what
  `honoursConfiguredTimeZone` in the test suite pins down.

## Clean up

```bash
docker compose -f examples/04-kafka-connect-s3-partitioner/docker/docker-compose.yml down -v
```

## Licence

The partitioner is Copyright (C) 2020 Can Elmas, licensed under the Apache License 2.0. The
full text is in `LICENSE`, and the original headers are kept on every source file.

package de.kafkastreams.fraud

import java.time.{Duration, Instant}
import java.util.Properties
import java.util.concurrent.CountDownLatch

import scala.util.{Failure, Success, Try}

import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

/**
 * Runs the fraud-detection topology against a real Kafka cluster and prints what its state store currently holds.
 *
 * Start the broker with the Docker Compose file in `docker/`, seed some traffic with `SeedProducer`, and watch the
 * alerts appear.
 */
object Main {

  private val ApplicationId = "de-02-kafka-streams-fraud"

  def main(args: Array[String]): Unit = {
    val bootstrapServers = KafkaSettings.bootstrapServers
    println(s"Connecting to Kafka at $bootstrapServers")

    val streams = new KafkaStreams(FraudTopology.build(), streamsProperties(bootstrapServers))
    val stopped = new CountDownLatch(1)

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      streams.close(Duration.ofSeconds(10))
      stopped.countDown()
    }))

    streams.start()
    println(s"Topology started. Alerts are written to '${FraudTopology.AlertsTopic}'. Press Ctrl+C to stop.")

    while (stopped.getCount > 0) {
      printStoreSnapshot(streams)
      stopped.await(10L, java.util.concurrent.TimeUnit.SECONDS)
    }
  }

  /** Shows the interactive-query result, or why it is not available yet. */
  private def printStoreSnapshot(streams: KafkaStreams): Unit = {
    val snapshot = Try(StoreQueries.recentDeclines(streams, Duration.ofHours(1), Instant.now()))
    snapshot match {
      case Success(rows) =>
        println(s"Declines per customer (state ${streams.state()}):")
        println(StoreQueries.render(rows))
      case Failure(error) =>
        println(s"State store not queryable yet (${streams.state()}): ${error.getMessage}")
    }
  }

  private def streamsProperties(bootstrapServers: String): Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, ApplicationId)
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[StringSerde].getName)
    // Every Kafka Streams application needs its own directory for its RocksDB
    // state stores. Giving it an explicit, example-specific name keeps the
    // examples in this repository from stepping on each other.
    properties.put(StreamsConfig.STATE_DIR_CONFIG, KafkaSettings.stateDir)
    // Read topics from the beginning so a restart replays the demo traffic.
    properties.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest")
    // A single broker means a single replica; the defaults assume three.
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, Integer.valueOf(1))
    // Suppression is what controls emission in this example, so the extra
    // caching layer would only delay output without changing the result.
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, java.lang.Long.valueOf(0L))
    properties
  }
}

/** Where the Kafka broker and the local state live, both overridable by environment variable. */
object KafkaSettings {

  /** Matches the host port published by `docker/docker-compose.yml`. */
  val defaultBootstrapServers = "localhost:10292"

  def bootstrapServers: String =
    sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", defaultBootstrapServers)

  def stateDir: String =
    sys.env.getOrElse(
      "KAFKA_STREAMS_STATE_DIR",
      s"${System.getProperty("java.io.tmpdir")}/de-02-kafka-streams-fraud"
    )
}

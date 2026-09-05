package de.kafka.ops

import org.apache.kafka.clients.admin.*
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource

import java.util.Optional
import scala.jdk.CollectionConverters.*

/**
 * Everything this example does against a live cluster, in one place.
 *
 * `AdminClient` is the Java interface Apache Kafka ships for administration: creating topics, reading and changing
 * configuration, listing consumer groups, moving partitions between brokers. The command-line tools in a Kafka
 * distribution (`kafka-topics.sh` and friends) are thin wrappers around exactly these calls, and so is much of what a
 * console such as CMAK shows.
 *
 * Every method here returns the plain data types from the rest of this package rather than Kafka classes. That is what
 * lets the analysis and the printing be unit-tested with hand-written numbers while this file stays a thin,
 * untested-by-design translation layer.
 *
 * `AdminClient` calls are asynchronous and return a `KafkaFuture`. This class waits for each one, because an operator
 * script is a sequence of steps and there is nothing useful to do in between.
 */
final class KafkaOps(admin: Admin) extends AutoCloseable {

  /** Broker identifiers currently registered in the cluster, sorted. */
  def brokerIds(): List[Int] =
    admin.describeCluster().nodes().get().asScala.map(_.id).toList.sorted

  /** Topic names, with Kafka's own bookkeeping topics (whose names start with an underscore) left out. */
  def userTopicNames(): List[String] =
    admin.listTopics().names().get().asScala.toList.filterNot(_.startsWith("_")).sorted

  /**
   * Creates the topics of a plan that do not exist yet, and reports which ones were created.
   *
   * Existing topics are left untouched: changing the partition count of a live topic re-routes keys to different
   * partitions and is never something to do by accident.
   */
  def createMissingTopics(plans: List[TopicPlan]): List[String] = {
    val existing = admin.listTopics().names().get().asScala.toSet
    val missing  = plans.filterNot(plan => existing.contains(plan.name))

    if (missing.nonEmpty) {
      val requests = missing.map { plan =>
        new NewTopic(plan.name, plan.partitions, plan.replicationFactor).configs(plan.configs.asJava)
      }
      admin.createTopics(requests.asJava).all().get()
    }
    missing.map(_.name)
  }

  /** The replication state of every partition of the given topics, ready for `ReplicationHealth.report`. */
  def describeReplication(topics: List[String]): List[PartitionReplicaState] =
    admin
      .describeTopics(topics.asJava)
      .allTopicNames()
      .get()
      .asScala
      .toList
      .flatMap { case (topic, description) =>
        description.partitions().asScala.map { partition =>
          PartitionReplicaState(
            ref = PartitionRef(topic, partition.partition()),
            leader = Option(partition.leader()).filterNot(_.isEmpty).map(_.id),
            replicas = partition.replicas().asScala.map(_.id).toList,
            inSyncReplicas = partition.isr().asScala.map(_.id).toList
          )
        }
      }

  /** Current replica placement of one topic: partition number to broker identifiers, leader first. */
  def currentAssignment(topic: String): Map[Int, List[Int]] =
    describeReplication(List(topic)).map(state => state.ref.partition -> state.replicas).toMap

  /** The effective configuration of a topic, including the values Kafka defaulted rather than the operator set. */
  def topicConfig(topic: String): Map[String, String] = {
    val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    admin
      .describeConfigs(List(resource).asJava)
      .all()
      .get()
      .get(resource)
      .entries()
      .asScala
      .map(entry => entry.name -> entry.value)
      .toMap
  }

  /**
   * Changes single topic settings, leaving every other setting alone.
   *
   * `incrementalAlterConfigs` is used rather than the older `alterConfigs`, which replaced the whole configuration and
   * therefore reset anything the caller forgot to repeat.
   */
  def alterTopicConfig(topic: String, changes: Map[String, String]): Unit = {
    val resource   = new ConfigResource(ConfigResource.Type.TOPIC, topic)
    val operations = changes.map { case (key, value) =>
      new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET)
    }
    admin.incrementalAlterConfigs(Map(resource -> operations.asJavaCollection).asJava).all().get()
  }

  /** Identifiers of every consumer group the cluster knows about. */
  def consumerGroupIds(): List[String] =
    admin.listConsumerGroups().all().get().asScala.map(_.groupId).toList.sorted

  /**
   * Lag of one consumer group, computed from two reads:
   *
   *   - the offsets the group has committed, and
   *   - the end offset of each of those partitions.
   *
   * The two reads happen a few milliseconds apart, so the result is a snapshot of a moving target - which is exactly
   * what every lag dashboard shows, and why `ConsumerLag` clamps a negative difference to zero.
   */
  def groupLag(group: String): GroupLag = {
    val committed: Map[TopicPartition, OffsetAndMetadata] =
      admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().asScala.toMap

    val endOffsetRequest = committed.keys.map(tp => tp -> OffsetSpec.latest()).toMap
    val endOffsets       = admin.listOffsets(endOffsetRequest.asJava).all().get().asScala

    val offsets = committed.toList.map { case (tp, offsetAndMetadata) =>
      PartitionOffsets(
        ref = PartitionRef(tp.topic, tp.partition),
        endOffset = endOffsets.get(tp).map(_.offset).getOrElse(0L),
        committedOffset = Option(offsetAndMetadata).map(_.offset)
      )
    }
    ConsumerLag.forGroup(group, offsets)
  }

  /**
   * Asks the cluster to move the given partitions onto new brokers.
   *
   * The call only registers the plan; the brokers then copy the partition logs in the background.
   * `reassignmentsInProgress` reports how far that has got.
   */
  def startReassignment(topic: String, changes: Map[Int, List[Int]]): Unit =
    if (changes.nonEmpty) {
      val request = changes.map { case (partition, replicas) =>
        val reassignment = new NewPartitionReassignment(replicas.map(Integer.valueOf).asJava)
        new TopicPartition(topic, partition) -> Optional.of(reassignment)
      }
      admin.alterPartitionReassignments(request.asJava).all().get()
    }

  /**
   * Partitions still being moved, with the brokers each one is being added to. Empty once the cluster has caught up.
   */
  def reassignmentsInProgress(): Map[PartitionRef, List[Int]] =
    admin
      .listPartitionReassignments()
      .reassignments()
      .get()
      .asScala
      .map { case (tp, reassignment) =>
        PartitionRef(tp.topic, tp.partition) -> reassignment.addingReplicas().asScala.map(_.intValue).toList
      }
      .toMap

  override def close(): Unit = admin.close()
}

object KafkaOps {

  /** Opens an `AdminClient` against the given bootstrap servers, for example `localhost:11301`. */
  def connect(bootstrapServers: String): KafkaOps = {
    val properties = new java.util.Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000")
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000")
    new KafkaOps(Admin.create(properties))
  }
}

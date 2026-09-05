package de.kafka.ops

/** One partition of one topic, identified the way Apache Kafka identifies it: by topic name and partition number. */
final case class PartitionRef(topic: String, partition: Int) {
  override def toString: String = s"$topic-$partition"
}

/**
 * The two numbers a lag calculation needs, read from the cluster for a single partition.
 *
 * @param endOffset
 *   the offset the next record written to this partition will get, also called the "log end offset". It is the number
 *   of records the partition has ever held, minus whatever retention has already deleted.
 * @param committedOffset
 *   the offset a consumer group has stored as "everything below this is processed". `None` means the group has never
 *   committed anything for this partition, which happens for a brand-new group.
 */
final case class PartitionOffsets(
    ref: PartitionRef,
    endOffset: Long,
    committedOffset: Option[Long]
)

/**
 * How far a consumer group is behind on one partition.
 *
 * @param lag
 *   number of records written but not yet marked as processed.
 */
final case class PartitionLag(
    ref: PartitionRef,
    endOffset: Long,
    committedOffset: Option[Long],
    lag: Long
)

/** The same picture for a whole consumer group, one entry per partition it is assigned. */
final case class GroupLag(group: String, partitions: List[PartitionLag]) {

  /** Records outstanding across every partition: the single number an alert usually watches. */
  def totalLag: Long = partitions.map(_.lag).sum

  /** The partition furthest behind, which is where an investigation starts. `None` for a group with no partitions. */
  def worstPartition: Option[PartitionLag] = partitions.maxByOption(_.lag)

  /** Partitions the group has never committed an offset for. */
  def uncommittedPartitions: List[PartitionLag] = partitions.filter(_.committedOffset.isEmpty)
}

/**
 * Turns raw offsets into lag.
 *
 * This is deliberately a pure calculation with no Kafka types in sight: everything here can be unit-tested by handing
 * it numbers, and the code that fetches those numbers from a broker lives in `KafkaOps`.
 */
object ConsumerLag {

  /**
   * Lag for one partition.
   *
   * Two cases need care:
   *
   *   - A group that has never committed is treated as being behind by the whole partition, because it will have to
   *     read everything that is still retained.
   *   - A committed offset above the end offset can be observed while the two numbers are fetched a few milliseconds
   *     apart. It is clamped to zero rather than reported as negative lag, which would only confuse a dashboard.
   */
  def forPartition(offsets: PartitionOffsets): PartitionLag = {
    val lag = offsets.committedOffset match {
      case Some(committed) => math.max(0L, offsets.endOffset - committed)
      case None            => math.max(0L, offsets.endOffset)
    }
    PartitionLag(offsets.ref, offsets.endOffset, offsets.committedOffset, lag)
  }

  /** Lag for a whole group, sorted by partition so that two runs print in the same order. */
  def forGroup(group: String, offsets: List[PartitionOffsets]): GroupLag =
    GroupLag(
      group = group,
      partitions = offsets
        .map(forPartition)
        .sortBy(entry => (entry.ref.topic, entry.ref.partition))
    )
}

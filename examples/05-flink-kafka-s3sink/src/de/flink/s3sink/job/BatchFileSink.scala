package de.flink.s3sink.job

import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy

import java.io.OutputStream
import java.nio.charset.StandardCharsets

/**
 * Builds the sink that writes finished batches to S3-compatible storage.
 *
 * `FileSink` is the replacement for the deprecated `StreamingFileSink`. It is a two-phase sink: while a file is being
 * written it is "in progress" and carries a hidden name that does not start with `part-`, so readers ignore it. Only
 * when a checkpoint completes does Flink rename it to its final `part-...` name. Combined with exactly-once
 * checkpointing that means a downstream reader never sees a half-written file, and a job restart never leaves a
 * duplicate behind.
 *
 * The directory a record lands in comes from the pure `BucketPath` function and travels with the record itself, so the
 * sink stays a mechanical detail with no knowledge of the domain.
 */
object BatchFileSink {

  /** Writes one JSON object per line. */
  @SerialVersionUID(1L)
  private final class JsonLineEncoder extends Encoder[OrderRecords.Outgoing] {
    override def encode(record: OrderRecords.Outgoing, stream: OutputStream): Unit = {
      stream.write(record.f1.getBytes(StandardCharsets.UTF_8))
      stream.write('\n')
    }
  }

  /** Reads the directory that the windowing operator already computed. */
  @SerialVersionUID(1L)
  private final class PrecomputedBucketAssigner extends BucketAssigner[OrderRecords.Outgoing, String] {
    override def getBucketId(record: OrderRecords.Outgoing, context: BucketAssigner.Context): String = record.f0

    override def getSerializer: SimpleVersionedSerializer[String] = SimpleVersionedStringSerializer.INSTANCE
  }

  /**
   * The rolling policy decides when the file that is currently being written is closed and a new one is started.
   * Closing on every checkpoint is what makes the output exactly-once: a part file becomes visible at the same moment
   * the checkpoint that produced it becomes durable, so a restart can never publish a record twice and never lose one.
   * The checkpoint interval is therefore also the delay before finished batches appear in the bucket, and it decides
   * how many files are produced -- object stores dislike millions of tiny ones, so do not set it to one second.
   *
   * @param outputUri
   *   base location, for example `s3://orders/customer-batches`
   */
  def apply(outputUri: String): FileSink[OrderRecords.Outgoing] =
    FileSink
      .forRowFormat(new Path(outputUri), new JsonLineEncoder)
      .withBucketAssigner(new PrecomputedBucketAssigner)
      .withRollingPolicy(OnCheckpointRollingPolicy.build[OrderRecords.Outgoing, String]())
      .build()
}

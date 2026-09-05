package de.presto.hive

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  CreateBucketRequest,
  Delete,
  DeleteObjectsRequest,
  ListObjectsV2Request,
  ObjectIdentifier,
  PutObjectRequest
}

import java.net.URI
import java.nio.file.Path as JPath
import scala.jdk.CollectionConverters.*
import scala.util.Using

/**
 * Where the Parquet files live once they leave the local disk.
 *
 * @param endpoint
 *   the S3 API address. MinIO speaks the Amazon S3 protocol, so the same client library works unchanged against it.
 * @param bucket
 *   the bucket the table lives in.
 * @param accessKey
 *   S3 access key identifier. Development credentials only; a real deployment uses a role or a secret manager.
 * @param prefix
 *   the key prefix inside the bucket that becomes the table's location.
 */
final case class ObjectStoreConfig(
    endpoint: String,
    bucket: String,
    accessKey: String,
    secretKey: String,
    prefix: String
) {

  /**
   * The table location as PrestoDB's Hive connector wants to see it.
   *
   * `s3a` is the scheme Hadoop-based tools historically used for Amazon S3. PrestoDB maps `s3`, `s3a` and `s3n` to the
   * same filesystem implementation, so any of them works; `s3a` is used here because it is what the Hive world writes.
   */
  def tableLocation: String = s"s3a://$bucket/$prefix"
}

/** Uploads the locally written Parquet tree into an S3-compatible object store. */
object ObjectStore {

  /** Opens a client, hands it to `use`, and closes it afterwards even if `use` throws. */
  def withClient[A](config: ObjectStoreConfig)(use: S3Client => A): A =
    Using.resource(open(config))(use)

  private def open(config: ObjectStoreConfig): S3Client =
    S3Client
      .builder()
      .endpointOverride(URI.create(config.endpoint))
      // MinIO has no notion of a region, but the AWS SDK insists on one being set.
      .region(Region.US_EAST_1)
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(config.accessKey, config.secretKey))
      )
      // Path-style access puts the bucket in the path (`http://host/bucket/key`) instead of
      // in the hostname (`http://bucket.host/key`). Virtual-host style needs wildcard DNS,
      // which a local container does not have.
      .forcePathStyle(true)
      .build()

  /** Creates the bucket unless it already exists. */
  def ensureBucket(client: S3Client, bucket: String): Unit = {
    val exists = client.listBuckets().buckets().asScala.exists(_.name == bucket)
    if (!exists) client.createBucket(CreateBucketRequest.builder().bucket(bucket).build()): Unit
  }

  /**
   * Removes every object under the table prefix.
   *
   * Re-running the example must not mix new files with the ones the previous run left behind, because the table would
   * then contain each event twice and every number in the report would be wrong.
   *
   * The prefix is matched with a trailing slash so that clearing `clickstream` cannot also delete a neighbouring
   * `clickstream-archive`. Object stores match key prefixes as plain text, with no idea that a slash means anything.
   */
  def clearPrefix(client: S3Client, bucket: String, prefix: String): Int = {
    val keys = client
      .listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(bucket).prefix(s"$prefix/").build())
      .contents()
      .asScala
      .map(_.key)
      .toVector
    keys.grouped(1000).foreach { batch =>
      val identifiers = batch.map(key => ObjectIdentifier.builder().key(key).build()).asJava
      client.deleteObjects(
        DeleteObjectsRequest
          .builder()
          .bucket(bucket)
          .delete(Delete.builder().objects(identifiers).build())
          .build()
      ): Unit
    }
    keys.size
  }

  /**
   * Uploads every file under `root`, keeping the directory layout as the object key layout.
   *
   * That correspondence is the whole trick behind a Hive table in object storage: object stores have no directories,
   * only keys, but a key that contains `country=DE/dt=2023-11-14/clicks.parquet` is treated by every Hive-compatible
   * engine as if those slashes were directories.
   *
   * @return
   *   the object keys that were written.
   */
  def uploadDirectory(client: S3Client, config: ObjectStoreConfig, root: JPath): Seq[String] =
    ParquetClickstreamWriter.relativeFilePaths(root).map { relative =>
      val key = s"${config.prefix}/$relative"
      client.putObject(
        PutObjectRequest.builder().bucket(config.bucket).key(key).build(),
        RequestBody.fromFile(root.resolve(relative))
      ): Unit
      key
    }
}

package de.parquet.arrow

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, ListObjectsV2Request, PutObjectRequest}

import java.net.URI
import java.nio.file.{Files, Path as JPath}
import scala.jdk.CollectionConverters.*
import scala.util.Using

/**
 * Connection details for an S3-compatible object store.
 *
 * @param endpoint
 *   the S3 API address. MinIO speaks the Amazon S3 protocol, so the same client library works unchanged against it and
 *   against Amazon S3 itself.
 * @param prefix
 *   the key prefix inside the bucket that the archive is copied under.
 */
final case class ObjectStoreConfig(
    endpoint: String,
    bucket: String,
    accessKey: String,
    secretKey: String,
    prefix: String
)

/** One object as the store reports it back. */
final case class StoredObject(key: String, sizeBytes: Long)

/**
 * Copies the finished archive into object storage.
 *
 * This is the only part of the example that needs a running container, and it is optional. It exists to make one point
 * concrete: nothing about Parquet or Arrow depends on a local disk. The bytes a Parquet writer produces are the same
 * bytes whether they land in a directory or in a bucket, which is why every lakehouse in this repository can keep its
 * tables in object storage and still be read by any engine.
 */
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
      // Path-style access puts the bucket in the path (`http://host/bucket/key`) rather
      // than in the hostname (`http://bucket.host/key`). Virtual-host style needs
      // wildcard DNS, which a local container does not have.
      .forcePathStyle(true)
      .build()

  /** Creates the bucket unless it already exists. */
  def ensureBucket(client: S3Client, bucket: String): Unit = {
    val exists = client.listBuckets().buckets().asScala.exists(_.name == bucket)
    if (!exists) client.createBucket(CreateBucketRequest.builder().bucket(bucket).build()): Unit
  }

  /** Uploads every regular file directly inside `directory`, keeping the file names as object keys. */
  def uploadDirectory(client: S3Client, config: ObjectStoreConfig, directory: JPath): Seq[StoredObject] =
    Using.resource(Files.list(directory)) { entries =>
      entries
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .toVector
        .sortBy(_.getFileName.toString)
        .map { file =>
          val key = s"${config.prefix}/${file.getFileName}"
          client.putObject(
            PutObjectRequest.builder().bucket(config.bucket).key(key).build(),
            RequestBody.fromFile(file)
          ): Unit
          StoredObject(key, Files.size(file))
        }
    }

  /** Lists what is currently stored under the configured prefix. */
  def list(client: S3Client, config: ObjectStoreConfig): Seq[StoredObject] =
    client
      .listObjectsV2(ListObjectsV2Request.builder().bucket(config.bucket).prefix(config.prefix).build())
      .contents()
      .asScala
      .toVector
      .map(entry => StoredObject(entry.key, entry.size))
      .sortBy(_.key)
}

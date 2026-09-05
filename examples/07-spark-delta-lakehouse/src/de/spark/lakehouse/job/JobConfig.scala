package de.spark.lakehouse.job

import de.spark.lakehouse.core.LakehouseLayout

/**
 * Everything the job needs to know about the outside world.
 *
 * Reading configuration is deliberately confined to this one file. The transformation code never asks an environment
 * variable a question, which is what lets the tests run it with no environment at all.
 *
 * @param layout
 *   where the tables live
 * @param objectStore
 *   connection details for S3-compatible storage, or `None` when the warehouse is a plain local directory
 * @param orderCount
 *   how many synthetic orders to generate for the bronze layer
 */
final case class JobConfig(
    layout: LakehouseLayout,
    objectStore: Option[ObjectStoreConfig],
    orderCount: Int
)

/**
 * Credentials and endpoint for an S3-compatible object store such as MinIO or Amazon S3.
 *
 * @param endpoint
 *   the base address of the service, for example `http://minio:9000`
 * @param accessKey
 *   the user name half of the credentials
 * @param secretKey
 *   the password half of the credentials
 * @param pathStyleAccess
 *   when true, buckets are addressed as `endpoint/bucket` instead of `bucket.endpoint`. MinIO needs this; Amazon S3
 *   does not.
 */
final case class ObjectStoreConfig(
    endpoint: String,
    accessKey: String,
    secretKey: String,
    pathStyleAccess: Boolean
)

object JobConfig {

  /** Warehouse root used when nothing is configured: a local directory, so the example runs with no infrastructure. */
  val DefaultWarehouseRoot = "out/lakehouse-07"

  val DefaultOrderCount = 400

  /**
   * Builds the configuration from environment variables, falling back to a purely local run.
   *
   *   - `LAKEHOUSE_ROOT` - warehouse root path or uniform resource identifier (URI)
   *   - `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` - object store connection, only consulted when the warehouse
   *     root starts with `s3a://`
   *   - `ORDER_COUNT` - how many synthetic orders to generate
   *
   * @param env
   *   the environment to read; passed in rather than read directly so tests can supply their own map
   */
  def fromEnvironment(env: Map[String, String]): JobConfig = {
    val root = env.getOrElse("LAKEHOUSE_ROOT", DefaultWarehouseRoot)

    val objectStore =
      if (root.startsWith("s3a://"))
        Some(
          ObjectStoreConfig(
            endpoint = env.getOrElse("S3_ENDPOINT", "http://localhost:10700"),
            accessKey = env.getOrElse("S3_ACCESS_KEY", "minioadmin"),
            secretKey = env.getOrElse("S3_SECRET_KEY", "minioadmin"),
            pathStyleAccess = true
          )
        )
      else None

    JobConfig(
      layout = LakehouseLayout(root),
      objectStore = objectStore,
      orderCount = positiveIntOr(env.get("ORDER_COUNT"), DefaultOrderCount)
    )
  }

  private def positiveIntOr(raw: Option[String], fallback: Int): Int =
    raw.flatMap(value => scala.util.Try(value.trim.toInt).toOption).filter(_ > 0).getOrElse(fallback)
}

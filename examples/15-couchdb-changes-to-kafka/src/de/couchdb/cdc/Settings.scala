package de.couchdb.cdc

/** Where CouchDB lives, for example `http://localhost:11598`. */
final case class CouchDbUrl(value: String) extends AnyVal

/** The name of a CouchDB database, which is also the first path segment of every request against it. */
final case class DatabaseName(value: String) extends AnyVal

/** The administrator account the example connects with. */
final case class Credentials(user: String, password: String)

/** The `host:port` list an Apache Kafka client bootstraps from. */
final case class BootstrapServers(value: String) extends AnyVal

/** An Apache Kafka topic name. */
final case class TopicName(value: String) extends AnyVal

/**
 * Everything the connector needs to be told, with defaults that match the Docker Compose stack in `docker/`.
 *
 * Each value can be overridden with an environment variable, so the same build also runs against a CouchDB or a broker
 * somewhere else.
 */
final case class Settings(
    couchDbUrl: CouchDbUrl,
    database: DatabaseName,
    credentials: Credentials,
    bootstrapServers: BootstrapServers,
    topic: TopicName,
    feedTimeoutMillis: Int,
    heartbeatMillis: Int,
    checkpointEveryNChanges: Int
)

object Settings {

  /**
   * @param feedTimeoutMillis
   *   how long CouchDB keeps one continuous `_changes` response open when nothing is happening. The connector simply
   *   reconnects afterwards, and because reconnecting is where it notices a shutdown request, this value also bounds
   *   how long Ctrl+C takes to take effect.
   * @param heartbeatMillis
   *   how often CouchDB writes a blank line into an idle response, which stops proxies and firewalls from dropping a
   *   connection they believe to be dead.
   */
  val defaults: Settings = Settings(
    couchDbUrl = CouchDbUrl("http://localhost:11598"),
    database = DatabaseName("catalogue"),
    credentials = Credentials("admin", "couchdb"),
    bootstrapServers = BootstrapServers("localhost:11592"),
    topic = TopicName("catalogue.products"),
    feedTimeoutMillis = 30000,
    heartbeatMillis = 10000,
    checkpointEveryNChanges = 5
  )

  /** The defaults, with any `COUCHDB_*` / `KAFKA_*` environment variable that is set taking precedence. */
  def fromEnvironment(environment: Map[String, String]): Settings =
    defaults.copy(
      couchDbUrl = environment.get("COUCHDB_URL").map(CouchDbUrl.apply).getOrElse(defaults.couchDbUrl),
      database = environment.get("COUCHDB_DATABASE").map(DatabaseName.apply).getOrElse(defaults.database),
      credentials = Credentials(
        user = environment.getOrElse("COUCHDB_USER", defaults.credentials.user),
        password = environment.getOrElse("COUCHDB_PASSWORD", defaults.credentials.password)
      ),
      bootstrapServers =
        environment.get("KAFKA_BOOTSTRAP_SERVERS").map(BootstrapServers.apply).getOrElse(defaults.bootstrapServers),
      topic = environment.get("KAFKA_TOPIC").map(TopicName.apply).getOrElse(defaults.topic)
    )
}

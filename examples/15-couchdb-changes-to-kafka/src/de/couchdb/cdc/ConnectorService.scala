package de.couchdb.cdc

import ox.Ox

/** Reports progress on standard output, the only place an example like this needs it. */
object ConsoleLog extends ConnectorLog {
  def note(message: String): Unit = println(message)
}

/**
 * The long-running connector: follow CouchDB's `_changes` feed, publish every applicable row to Apache Kafka, and
 * remember where it got to.
 *
 * The shape is deliberately simple. One CouchDB response is one "connection"; the response ends when CouchDB's feed
 * timeout expires, and the loop opens the next one starting from the bookmark reached so far. A normally completed
 * connection is flushed before reconnecting. On Ctrl+C, Ox interrupts the blocking read and closes everything
 * registered with `useCloseableInScope` in reverse order. Interruption does not promise a final remote checkpoint
 * write; any acknowledged progress since the last stored bookmark is deliberately replayed on restart.
 */
object ConnectorService {

  def run(settings: Settings, log: ConnectorLog)(using Ox): Progress = {
    val client      = CouchDbClient.open(settings)
    val checkpoints = new CouchDbCheckpointStore(client)
    KafkaChangeSink.createTopicIfAbsent(settings, partitions = 1, replication = 1)
    val processor =
      new ChangeProcessor(KafkaChangeSink.open(settings), checkpoints, log, settings.checkpointEveryNChanges)

    val resumeFrom = checkpoints.load()
    log.note(s"following ${settings.database.value} from sequence ${resumeFrom.checkpoint.since.value}")

    var progress = Progress.startingAt(resumeFrom)
    while (!Thread.currentThread().isInterrupted)
      progress = followOneConnection(client, processor, progress)
    log.note(s"shutting down; ${progress.summary}")
    processor.flush(progress)
  }

  /** Reads one continuous `_changes` response to its end, then makes sure the bookmark is on disk. */
  private def followOneConnection(client: CouchDbClient, processor: ChangeProcessor, start: Progress): Progress = {
    var progress = start
    client.readChanges(start.stored.checkpoint.since) { line =>
      progress = processor.handleLine(progress, line)
    }
    processor.flush(progress)
  }
}

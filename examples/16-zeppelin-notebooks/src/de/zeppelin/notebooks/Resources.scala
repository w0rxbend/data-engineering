package de.zeppelin.notebooks

import java.io.InputStream
import java.nio.charset.StandardCharsets

/**
 * Reads the files this example ships on its classpath.
 *
 * The notebooks and the interpreter configuration are the deliverable of this example: the Docker Compose stack
 * installs the very same files into the Apache Zeppelin container, and the unit tests read them from the classpath. One
 * copy, checked by the tests and used by the container.
 */
object Resources {

  /** Reads a classpath resource as text, failing loudly when it is missing. */
  def text(path: String): String = {
    val stream = Option(getClass.getResourceAsStream(path))
      .getOrElse(throw new IllegalArgumentException(s"resource '$path' is not on the classpath"))
    try new String(stream.readAllBytes(), StandardCharsets.UTF_8)
    finally close(stream)
  }

  private def close(stream: InputStream): Unit = stream.close()
}

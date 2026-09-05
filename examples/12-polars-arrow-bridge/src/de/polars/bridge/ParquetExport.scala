package de.polars.bridge

import org.apache.arrow.dataset.file.{DatasetFileWriter, FileFormat}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowFileReader

import java.io.FileInputStream
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*
import scala.util.Using

/**
 * Turns an Arrow IPC file into an Apache Parquet dataset.
 *
 * Parquet and Arrow solve different halves of the same problem. Arrow is the *in-memory* layout: uncompressed, aligned,
 * ready for the processor. Parquet is the *at-rest* layout: compressed, encoded per column, with statistics per row
 * group so a reader can skip whole chunks. The archive on disk should be Parquet; the buffer handed between two
 * processes on one machine should be Arrow.
 *
 * The conversion is done by `arrow-dataset`, which calls into the same native Arrow C++ code Polars' own Parquet reader
 * is built on. The alternative on the JVM is the classic `parquet-mr` writer, which pulls in a large part of Apache
 * Hadoop for the sake of writing one local file.
 */
object ParquetExport {

  /**
   * Reads `arrowIpcFile` and writes its rows as Parquet into `targetDirectory`.
   *
   * Parquet output is a *directory*, not a single file: a dataset writer is free to split large input across several
   * files, and `baseNameTemplate` says how they are named. `{i}` is replaced by the file number.
   *
   * @return
   *   the Parquet files that were produced
   */
  def fromArrowIpc(allocator: BufferAllocator, arrowIpcFile: Path, targetDirectory: Path): List[Path] = {
    replaceDirectory(targetDirectory) { stagingDirectory =>
      Using.resource(new FileInputStream(arrowIpcFile.toFile)) { input =>
        Using.resource(new ArrowFileReader(input.getChannel, allocator)) { reader =>
          DatasetFileWriter.write(
            allocator,
            reader,
            FileFormat.PARQUET,
            stagingDirectory.toUri.toString,
            Array.empty[String],
            1,
            "order_lines_{i}.parquet"
          )
        }
      }
      require(parquetFilesIn(stagingDirectory).nonEmpty, "the native dataset writer produced no Parquet files")
    }
    parquetFilesIn(targetDirectory)
  }

  /** The Parquet files currently in a dataset directory, sorted by name. */
  def parquetFilesIn(directory: Path): List[Path] =
    Using.resource(Files.list(directory)) { entries =>
      entries
        .filter(path => path.getFileName.toString.endsWith(".parquet"))
        .sorted()
        .toArray
        .toList
        .map(_.asInstanceOf[Path])
    }

  /**
   * Builds a complete dataset beside the target before replacing the old directory.
   *
   * A conversion failure leaves the old dataset untouched. Directory replacement itself is deliberately best-effort:
   * Java exposes no portable atomic replacement for a non-empty directory, so a process or filesystem failure after the
   * old directory is removed requires rerunning this deterministic export.
   */
  private def replaceDirectory(target: Path)(write: Path => Unit): Unit = {
    val absolute = target.toAbsolutePath
    val parent   = absolute.getParent
    Files.createDirectories(parent)
    val staging = Files.createTempDirectory(parent, s".${absolute.getFileName}.")
    try {
      write(staging)
      deleteRecursively(absolute)
      Files.move(staging, absolute)
    } finally deleteRecursively(staging)
  }

  private def deleteRecursively(directory: Path): Unit =
    if (Files.exists(directory)) {
      Using.resource(Files.walk(directory)) { entries =>
        entries.iterator().asScala.toVector.reverse.foreach(path => Files.deleteIfExists(path): Unit)
      }
    }
}

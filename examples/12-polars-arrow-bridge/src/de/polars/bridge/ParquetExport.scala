package de.polars.bridge

import org.apache.arrow.dataset.file.{DatasetFileWriter, FileFormat}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowFileReader

import java.io.FileInputStream
import java.nio.file.{Files, Path}
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
    Files.createDirectories(targetDirectory)
    // The dataset writer refuses to write into a directory that already holds files, so that it can never half-replace
    // an existing dataset. Removing yesterday's files first makes re-running the example a full overwrite.
    parquetFilesIn(targetDirectory).foreach(Files.delete(_))
    Using.resource(new FileInputStream(arrowIpcFile.toFile)) { input =>
      Using.resource(new ArrowFileReader(input.getChannel, allocator)) { reader =>
        DatasetFileWriter.write(
          allocator,
          reader,
          FileFormat.PARQUET,
          targetDirectory.toUri.toString,
          Array.empty[String],
          1,
          "order_lines_{i}.parquet"
        )
      }
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
}

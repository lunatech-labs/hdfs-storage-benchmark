package lunatech.benchmarks

import java.io.PrintWriter

import org.apache.hadoop.conf.{ Configuration => HadoopConfiguration }
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
 *
 */
package object hdfs {

  /**
   * DataFrame decorator adding utility functions for benchmarking.
   *
   * @param data
   */
  implicit class TestDataFrame(val data: DataFrame) {

    /**
     * Reduces or increases the size of a data frame with the given ratio.
     *
     * @param ratio
     * @return
     */
    def scale(ratio: Double): DataFrame = {

      require(ratio > 0)
      require(data.count > 0)

      val i = ratio.toInt
      val r = ratio - i

      val p1 = if (i >= 1) (1 until i).foldLeft(data)((acc, i) => acc unionAll data) else data.limit(0)
      val p2 = data.limit((data.count * r).round.toInt)

      val rez = p1 unionAll p2
      rez
    }

    /**
     * Reduces or increases the size of a data frame to the given number of rows.
     *
     * @param records
     * @return
     */
    def scale(records: Int): DataFrame = {
      require(records > 0)
      require(data.count > 0)
      scale(records.toDouble / data.count)
    }

  }

  /**
   * Run a block and return the block result and the runtime in millis
   *
   * @param block
   * @return
   */
  def timeCode[T](block: => T): (T, Long) = {
    val start = new java.util.Date
    val result = block
    val runtime = (new java.util.Date).toInstant.toEpochMilli - start.toInstant.toEpochMilli
    (result, runtime)
  }

  def removeHdfsFile(path: String) = {
    val hdfs = FileSystem.get(new HadoopConfiguration())
    val workingPath = new Path(path)
    hdfs.delete(workingPath, true) // delete recursively
  }

  /**
   * Save a sequence of Strings as lines in an HDFS file
   *
   * @param rez
   * @param path
   * @return
   */
  def saveLinesToFile(rez: Iterable[String], path: String, extension: String = "", overwrite: Boolean = true) = {
    import java.io.BufferedOutputStream
    Try {
      // Create the HDFS file system handle
      val hdfs = FileSystem.get(new HadoopConfiguration())
      // Create a writer
      val writer = new PrintWriter(new BufferedOutputStream(hdfs.create(new Path(addExtension(path, extension)), overwrite)), true)
      //write each line
      rez.foreach(line => Try(writer.print(line + "\n")))
      // Close the streams
      Try(writer.close())
    }
  }

  /**
   * Set the extension to the given path preventing the double extension
   *
   * @param path
   * @param extension
   * @return
   */
  private[this] def addExtension(path: String, extension: String) = {
    val normExt = extension.trim
    val ext = if (extension.startsWith(".") || normExt.isEmpty) extension else "." + extension
    if (path.endsWith(ext)) path
    else path + ext
  }

}

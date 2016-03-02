package lunatech.benchmarks.hdfs

import com.typesafe.config.Config
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.SparkContext
import org.apache.spark.sql._

import scala.io.Source

/**
 * Test Bulk Reads and Writes
 */
object Benchmark extends SparkRunnable {

  val appName = "serial-killer"

  val requiredParameters = Seq(
    //    "input.file",
    "path.tmp",
    "start.size",
    "runs.per.step",
    "increment.size",
    "increment.steps"
  )

  def runJob(sc: SparkContext, config: Config) = {

    // Retrieve simple configuration parameters
    val tempDir = config.getString("path.tmp")
    val resultsFile = config.getString("results.file")
    val startSize = config.getInt("start.size")
    val runsPerStep = config.getInt("runs.per.step")
    val incrementSize = config.getInt("increment.size")
    val incrementSteps = config.getInt("increment.steps")
    val collectAverages = config.getBoolean("collect.averages")

    /**
     * Serializer to be tested in form of a map containing the serializer name/code as needed by the
     * DataFrame Reader and Writer as keys and additional options map as values.
     */
    val serializerDescriptions = Map(
      "com.databricks.spark.csv" ->
        Map[String, String]("header" -> "true"),
      "com.databricks.spark.avro" -> // uncompressed, snappy, deflate
        Map[String, String]("spark.sql.avro.compression.codec" -> "snappy"),
      "parquet" -> // uncompressed, snappy, gzip, lzo
        Map[String, String]("spark.sql.parquet.compression.codec" -> "snappy")
    )

    val sqlContext = new SQLContext(sc)

    // Create a data frame that will be the base (seed) for all the further generated data frames
    val seedData = createSeedData(sqlContext, tempDir, startSize)

    /**
     * Test the read and write times and run the benchmark 'runs' times.
     *
     * @param data
     * @param runs
     * @return
     */
    def test(data: DataFrame, runs: Int = 1) = {
      val records = data.count
      (0 until runs).flatMap { runNo =>
        serializerDescriptions.map {
          case (format, options) =>
            val dataRW = createRW(sqlContext, data, format, options)
            val (sizeSum, writeMSSum, read0MSSum, read1MSSum, read2MSSum) = testReadWriteTimes(sqlContext, format, dataRW, tempDir)
            Array(
              BenchmarkResult(runNo, format, "insert", records, writeMSSum / runs, sizeSum / runs),
              BenchmarkResult(runNo, format, "count all", records, read0MSSum / runs, sizeSum / runs),
              BenchmarkResult(runNo, format, "select all where", records, read1MSSum / runs, sizeSum / runs),
              BenchmarkResult(runNo, format, "select some cols where", records, read2MSSum / runs, sizeSum / runs)
            )
        }
      }
    }

    /**
     * Test the read and write times and return the averages for each set of parameters.
     *
     * @param data
     * @param runs
     * @return
     */
    def testAverages(data: DataFrame, runs: Int = 1) = {
      val records = data.count

      serializerDescriptions.map {
        case (format, options) =>
          val dataRW = createRW(sqlContext, data, format, options)
          val results = (0 until runs).map(_ => testReadWriteTimes(sqlContext, format, dataRW, tempDir))
          val (sizeSum, writeMSSum, read0MSSum, read1MSSum, read2MSSum) = results.reduce((r1, r2) => (r1._1 + r2._1, r1._2 + r2._2, r1._3 + r2._3, r1._4 + r2._4, r1._5 + r2._5))
          Array(
            BenchmarkResult(0, format, "insert", records, writeMSSum / runs, sizeSum / runs),
            BenchmarkResult(0, format, "count all", records, read0MSSum / runs, sizeSum / runs),
            BenchmarkResult(0, format, "select all where", records, read1MSSum / runs, sizeSum / runs),
            BenchmarkResult(0, format, "select some cols where", records, read2MSSum / runs, sizeSum / runs)
          )
      }
    }

    val header = f"| ${"Serializer"}%-32s | ${"Records"}%-12s | ${"Run #"}%-9s | ${"Size [MB]"}%-10s | ${"Write [s]"}%-10s | ${"Read 0 [s]"}%-10s | ${"Read 1 [s]"}%-10s | ${"Read 2 [s]"}%-10s |" ::
      f"| ${"----------"}%-32s | ${"------:"}%-12s | ${"----:"}%-9s | ${"--------:"}%-10s | ${"--------:"}%-10s | ${"--------:"}%-10s | ${"---------:"}%-10s | ${"---------:"}%-10s |" ::
      Nil
    header.foreach(log.info(_))

    val legend =
      """
        |Legend:
        ||Read 0 | SELECT COUNT(*) |
        ||Read 1 | SELECT COUNT(*) FROM SELECT * FROM data_frame WHERE C41 = 'pod.' AND C7 = 1 AND C8 = 0 AND C31 < 100 AND C35 > 0.1 AND C35 < 0.5 |
        ||Read 2 | SELECT COUNT(*) FROM SELECT "C1", "C2", "C3", "C7", "C8", "C31", "C35", "C41" FROM data_frame WHERE C41 = 'pod.' AND C7 = 1 AND C8 = 0 AND C31 < 100 AND C35 > 0.1 AND C35 < 0.5 |
      """.stripMargin

    val results =
      createTestDataFrames(seedData, incrementSteps, incrementSize).
        flatMap { df =>
          val results = if (collectAverages) testAverages(df, runsPerStep) else test(df, runsPerStep)
          results.map { bmrs =>
            val bmr = bmrs.head
            val line = f"| ${bmr.format}%-32s | ${bmr.records}%12d | ${bmr.runNo}%9d | ${bmr.fileSize.toDouble / 1024 / 1024}%10.2f | ${bmrs(0).timeMs / 1000.0}%10.3f | ${bmrs(1).timeMs / 1000.0}%10.3f | ${bmrs(2).timeMs / 1000.0}%10.3f | ${bmrs(3).timeMs / 1000.0}%10.3f |"
            log.info(line)
            line
          }
        }
    log.info(legend)

    saveLinesToFile(header ++ results :+ legend, resultsFile, "", true)
    logInfo(s"Bulk benchmark results were saved to $resultsFile.")
    (header ++ results).foreach(println)

  }

  /**
   * Convenience data type for readers and writers
   */
  type DataRW = (DataFrameReader, DataFrameWriter)

  /**
   * Convenience benchmark results bean
   *
   * @param runNo if the test was ran multiple times show the run attempt number
   * @param format serialization format (e.g. csv, avro, parquet...)
   * @param operation (insert, select, what sort of select...)
   * @param records how many records were used for benchmarking (key data)
   * @param timeMs how long did the operation take (key data)
   * @param fileSize how large was the file on disk (regardless of the hadoop block size)
   */
  case class BenchmarkResult(runNo: Int, format: String, operation: String, records: Long, timeMs: Long, fileSize: Long)

  /**
   * Create a tuple of (DataFrameReader, DataFrameWriter), aka DataRW
   *
   * @param sqlContext
   * @param data
   * @param format
   * @param options
   * @return
   */
  def createRW(sqlContext: SQLContext, data: DataFrame, format: String, options: Map[String, String]): DataRW = {
    val reader = options.toSeq.
      foldLeft(sqlContext.read.format(format))((acc, opt) => acc.option(opt._1, opt._2))

    val writer = options.toSeq.
      foldLeft(data.write.format(format))((acc, opt) => acc.option(opt._1, opt._2))

    (reader, writer)
  }

  /**
   * Create a test/sample sequence of DataFrame objects arranged from smaller to larger;
   *
   * @param steps how many times should the data frame be increased
   * @param incrementRatio how much should we increase at each step
   * @return
   */
  def createTestDataFrames(df0: DataFrame, steps: Int, incrementRatio: Double) = {

    require(steps > 0)
    require(incrementRatio > 0)

    (1 to steps).foldLeft(Seq[DataFrame]()) {
      case (stream, s) =>
        df0.scale(s * incrementRatio) +: stream
    }.reverse

  }

  /**
   * Create a test/sample sequence of DataFrame objects arranged from smaller to larger;
   *
   * @param steps how many times should the data frame be increased
   * @param incrementSize how many rows should we add at each step
   * @return
   */
  def createTestDataFrames(df0: DataFrame, steps: Int, incrementSize: Int) = {

    require(steps > 0)
    require(incrementSize > 0)

    (1 until steps).foldLeft(Seq[DataFrame](df0)) {
      case (stream, s) =>
        (stream.head unionAll df0.scale(incrementSize)) +: stream
    }.reverse

  }

  /**
   * Test the read and write speeds of a given DataFrameReader, DataFrameWriter tuple.
   *
   * @param sqlContext
   * @param dataRW a DataFrameReader, DataFrameWriter tuple
   * @param outputPath temporary path where to store the written data
   * @return
   */
  def testReadWriteTimes(sqlContext: SQLContext, format: String, dataRW: DataRW, outputPath: String) = {

    import java.util.UUID.randomUUID

    import org.apache.hadoop.conf.{ Configuration => HadoopConfiguration }

    val (dfr, dfw) = dataRW

    // Generate a random file name in the given path
    val outputFileName = s"$outputPath/bmt_${randomUUID.toString}"

    val hdfs = FileSystem.get(new HadoopConfiguration())
    val workingPath = new Path(outputFileName)

    // Get the bulk write time
    val writeTime = timeCode(dfw.save(outputFileName))._2

    // Get the bulk read time after aggregation (count)
    val readTime0 = timeCode { dfr.load(outputFileName).count }._2

    // Get the bulk read time after aggregation (count)
    val readTime1 = timeCode {
      dfr.load(outputFileName).
        filter("C41 = 'pod.' AND C7 = 1 AND C8 = 0 AND C31 < 100 AND C35 > 0.1 AND C35 < 0.5").count
    }._2

    // Get the bulk read time after aggregation (count)
    val readTime2 = timeCode {
      dfr.load(outputFileName).select("C1", "C2", "C3", "C7", "C8", "C31", "C35", "C41").
        filter("C41 = 'pod.' AND C7 = 1 AND C8 = 0 AND C31 < 100 AND C35 > 0.1 AND C35 < 0.5").count
    }._2

    val size = hdfs.getContentSummary(workingPath).getLength

    hdfs.delete(workingPath, true)

    (size, writeTime, readTime0, readTime1, readTime2)

  }

  def createSeedData(sqlContext: SQLContext, tempDir: String, startSize: Int) = {

    // A bit of ugly gymnastics here to get the typed data out of the file... I know...
    val dataLines = Source.fromInputStream(Benchmark.getClass.getResourceAsStream("/kddcup.data_01_percent"))
      .getLines.toSeq

    val file = s"tempDir/benchmark_input.tmp"

    sqlContext.sparkContext.parallelize(dataLines).saveAsTextFile(file)

    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(file)
      .scale(startSize)
  }

}

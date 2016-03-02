package lunatech.benchmarks.hdfs

import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.spark.{ Logging, SparkConf, SparkContext }
import spark.jobserver.{ SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation }

/**
 * Trivial trait for running basic Spark apps, both as stand alone apps and as Spark JobServer jobs.
 *
 */
trait SparkRunnable extends SparkJob with Logging {

  /**
   * This is the key for basically choosing a certain app and it should have
   * the form of 'app.....', reflected also in the configuration structure.
   *
   * @return
   */
  def appName: String

  /**
   * The list of required application parameters. If defined they are checked in the `validate()` function
   *
   * @return
   */
  def requiredParameters: Seq[String]

  /**
   * Any object extending this trait becomes a runnable application.
   * @param args
   */
  def main(implicit args: Array[String]): Unit = {
    val runnableName = this.getClass.getName
    logInfo(s"Running $runnableName")
    val sc = createDefaultSparkContext(runnableName)
    val config = ConfigFactory.parseString(args.mkString("\n")).
      withFallback(ConfigFactory.defaultReference()).getConfig(appName)
    logInfo(s"$appName: Application Parameters:\n${args.mkString("\n")}")
    logInfo(s"$appName: Configuration:\n${config.root.render()}")
    this.validate(sc, config) match {
      case SparkJobValid => this.runJob(sc, config)
      case SparkJobInvalid(message) => sys.error(message); sys.exit(-1)
    }
  }

  private def createDefaultSparkContext(runnerName: String) = {
    val defSparkConf = new SparkConf(true)
    val sparkConf = defSparkConf.setAppName(runnerName).
      setMaster(defSparkConf.get("spark.master", "local[*]"))
    new SparkContext(sparkConf)
  }

  /**
   * The default validate function that checks the requiredParameters are at least present.
   * @param sc
   * @param config
   * @return
   */
  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    requiredParameters.map(param => (param, config.hasPath(param))).filterNot(_._2).
      map(_._1) match {
        case Nil => SparkJobValid
        case missingParams => SparkJobInvalid(s"The following required parameters are missing: ${missingParams.mkString(", ")}")
      }
  }
}

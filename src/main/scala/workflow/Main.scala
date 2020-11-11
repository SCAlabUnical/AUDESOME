package workflow

import java.util
import java.util.logging.Logger
import scala.collection.JavaConverters._
import services.keywords.KeywordsExtraction
import services.fpm.TrajectoryExtraction
import services.parca.RoIExtraction
import factory.Factory
import dataServices.dao.{FlickrJsonDao,FlickrParquetDao, AbstractDao}
import org.rogach.scallop._
import java.nio.file.Paths

class WorkflowConfiguration(arguments: Seq[String]) extends ScallopConf(arguments) {
  val sparkHostname = opt[String](descr = "Endpoint of a spark cluster (empty if you want to use a local cluster)")
  val threadsCount = opt[Int](default = Option(4), descr = "Number of threads for spark driver in local execution")
  val numberOfPartitions = opt[Int](default = Option(8), descr = "Number of partitions used for Dataframe")
  val driverMemory = opt[String](default = Option("1g"), descr = "Amount of memory reserved for driver program application")
  val executorMemory = opt[String](default = Option("4g"),descr = "Amount of memory reserved for Spark executor")
  val sparkApplication = opt[String](default = Option("AUDESOME"), descr = "Name of current spark application")
  val datasetPath = opt[String](default = Option("src/main/resources/datasets/rome/rome.parquet"), descr = "Path to input dataset.")
  val stopWords = opt[String](default = Option("src/main/resources/stopWord/rome.txt"), descr = "Path to stop word's file.")
  val keywordsPath = opt[String](default = Option("src/main/resources/keywords/rome.txt"), descr = "Path to keyword's file.")
  val roisPath = opt[String](descr = "Path to computed roi's file.")
  val debugLevel = opt[Boolean](default = Option(true), descr = "Log level.")
  val limits = opt[Int](default = Option(50), descr = "Limit output.")
  verify()
}

object Main {

  org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.OFF)
  org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.OFF)

  private val logger = Logger.getLogger("AUDESOME_ENTRY_POINT")

  def readFile(filename: String): Seq[String] = {
    val bufferedSource = scala.io.Source.fromFile(filename)
    val lines = (for (line <- bufferedSource.getLines()) yield line).toList
    bufferedSource.close
    lines
  }

  def main(args: Array[String]) {
    val conf = new WorkflowConfiguration(args)
    logger.info("Strarting workflow using configuration %d".format(conf.threadsCount()))
    var sparkEndpoint = ""
    if (!conf.sparkHostname.isDefined)
      sparkEndpoint = s"local[${conf.threadsCount()}]"
    else
      sparkEndpoint = conf.sparkHostname()
    val spark = Factory.createSparkSession(conf.sparkApplication(), sparkEndpoint, conf.numberOfPartitions(), conf.driverMemory(), conf.executorMemory())
    if (conf.datasetPath.isDefined) {
      logger.info("datasetPath are: " + conf.datasetPath())
      val fileName = Paths.get(conf.datasetPath()).getFileName
      val extension = fileName.toString.split("\\.").last
      var dao: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]= null
      if (extension.equalsIgnoreCase("parquet"))
        dao = FlickrParquetDao(spark).readData(path = conf.datasetPath())
      else if (extension.equalsIgnoreCase("json"))
        dao = FlickrJsonDao(spark).readData(path = conf.datasetPath())
      var keywords = Set[String]()
      if (!conf.keywordsPath.isDefined) {
        logger.info("Computing keywords")
        keywords = KeywordsExtraction.computeRddLcurve(dataPath = conf.datasetPath(),
          spark = spark,
          stopWordPath = conf.stopWords(),
          cellSize = 200
        )
      }
      else{
        keywords = readFile(conf.keywordsPath()).toSet
      }
      logger.info("Keywords are:")
      keywords.foreach(x=>logger.warning(s"key: $x"))
      logger.info("Computing rois")
      val rois = RoIExtraction.automaticEpsDbcan(keywords.toSeq,dao, -1)
      logger.info("Rois are:")
      rois.foreach(x=>logger.warning(s"roi: $x"))
      val stringShapeMap: util.Map[String, String] = new util.HashMap[String, String]
      rois.foreach(x=>{
        stringShapeMap.put(x._1, x._2.toString)
      })
      logger.info("Computing trajectories")
      val trajectories = TrajectoryExtraction.computeTrajectoryUsingFPGrowth(df = dao, stringShapeMap = stringShapeMap.asScala)
      trajectories._2.foreach(x=>logger.warning(s"Trajectory: ${x}"))
    }

  }
}
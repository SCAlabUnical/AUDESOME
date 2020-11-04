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
  val sparkHostname = opt[String](default = Option("local[4]"))
  val sparkApplication = opt[String](default = Option("AUDESOME"))
  val datasetPath = opt[String](default = Option("src/main/resources/datasets/rome/FlickrRome2017-25.parquet"))
  val stopWords = opt[String](default = Option("src/main/resources/stopWord/rome.txt"))
  val keywordsPath = opt[String](default = Option("src/main/resources/keywords/rome.txt"))
  val roisPath = opt[String]()
  val debugLevel = opt[Boolean](default = Option(true))
  val limits = opt[Int](default = Option(50))
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
    logger.info("Strarting workflow using configuration %s".format(conf.toString()))
    val spark = Factory.createSparkSession(conf.sparkApplication(), conf.sparkHostname())
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
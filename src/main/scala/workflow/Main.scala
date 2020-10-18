package workflow

import java.util
import java.util.logging.Logger
import scala.collection.JavaConverters._
import services.keywords.KeywordsExtraction
import services.fpm.TrajectoryExtraction
import services.parca.RoIExtraction
import factory.Factory
import org.rogach.scallop._

class WorkflowConfiguration(arguments: Seq[String]) extends ScallopConf(arguments) {
  val sparkHostname = opt[String](default = Option("local[4]"))
  val sparkApplication = opt[String](default = Option("AUDESOME"))
  val datasetPath = opt[String](default = Option("src/main/resources/datasets/FlickrRome2017-25.json"))
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

  def main(args: Array[String]) {
    val conf = new WorkflowConfiguration(args)
    logger.info("Strarting workflow using configuration %s".format(conf.toString()))
    val spark = Factory.createSparkSession(conf.sparkApplication(), conf.sparkHostname())
    if (conf.datasetPath.isDefined) {
      logger.info("datasetPath are: " + conf.datasetPath())
      logger.info("Computing keywords")
      val results = KeywordsExtraction.computeRddLcurve(dataPath = conf.datasetPath(),
        spark = spark,
        stopWordPath = conf.stopWords(),
        cellSize = 200)
      logger.info("Keywords are:")
      results._1.foreach(x=>logger.warning(s"key: $x"))
      logger.info("Computing rois")
      val rois = RoIExtraction.automaticEpsDbcan(results._1.toSeq,results._2, -1)
      logger.info("Rois are:")
      rois.foreach(x=>logger.warning(s"roi: $x"))
      val stringShapeMap: util.Map[String, String] = new util.HashMap[String, String]
      rois.foreach(x=>{
        stringShapeMap.put(x._1, x._2.toString)
      })
      logger.info("Computing trajectories")
      val trajectories = TrajectoryExtraction.computeTrajectoryUsingFPGrowth(df = results._2, stringShapeMap = stringShapeMap.asScala)
      trajectories._2.foreach(x=>logger.warning(s"Trajectory: ${x}"))
    }

  }
}
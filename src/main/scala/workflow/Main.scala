package workflow

import java.util.logging.{Logger}
import dataServices.dao.{FlickrJsonDao,AbstractDao}
import factory.Factory

import org.rogach.scallop._

class WorkflowConfiguration(arguments: Seq[String]) extends ScallopConf(arguments) {
  val sparkHostname = opt[String](default = Option("local[4]"))
  val sparkApplication = opt[String](default = Option("AUDESOME"))
  val datasetPath = opt[String]()
  val keywordsPath = opt[String]()
  val roisPath = opt[String]()
  val debugLevel = opt[String](default = Option("INFO"))
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
    }

  }
}
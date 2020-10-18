package services.fpm

import java.io.PrintWriter

import com.google.gson.Gson
import org.apache.log4j.{Level, Logger}
import util.TestResults
import factory.Factory
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.{DataFrame, Row}
import trajectory.util.KMLUtils
import java.time.LocalDate
import java.time._
import java.time.format.DateTimeFormatter
import dataServices.transform.DataTransform
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth, PrefixSpan}
import workflow.utils.GeoUtils

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source

object TrajectoryExtraction {

  val defaultDataset = "/home/emanuele/Documents/Tesi/FlickrRome2017.json"
  val defaultKeywords = "/home/emanuele/IdeaProjects/SparkTest/input/services.keywords/keywordslcurve.txt"
  val appName = "TrajectoryExtraction"
  val kmlPath = "/home/emanuele/IdeaProjects/SparkTest/input/kml/Rome/refactoring/00Rome-Realshapes.kml"
  val datetime_format = DateTimeFormatter.ofPattern("MMM d, yyyy h:mm:ss a")
  def readRois(str: String): Array[(String, Geometry)] = {
    null
  }

  def mapCreateTrajectory(x: Row, shapeMap: mutable.Map[String, String]): Tuple2[UserTrajectory,Set[SingleTrajectory]] = {
    val lat = x.getDouble(0)
    val lon = x.getDouble(1)
    val username = x.getString(3)
    val daytime = LocalDate.parse(x.getString(4),datetime_format).toEpochDay
    val point = GeoUtils.getPoint(lon, lat)
    var arr: Array[SingleTrajectory] = Array[SingleTrajectory]()
    for ((places, polygon) <- shapeMap) {
      try {
        val pol = GeoUtils.getPolygonFromString(polygon)
        if (GeoUtils.isContained(point, pol)) {
          val trajectory = new SingleTrajectory(poi = places, daytime = LocalDate.parse(x.getString(4),DateTimeFormatter.ofPattern("MMM d, yyyy h:mm:ss a")).toEpochDay)
          arr = arr :+ trajectory
        }
      }
      catch {
        case x: Exception => {
          println(x)
        }
      }
    }
    (new UserTrajectory(username=username, daytime = daytime),arr.toSet)
  }

  def getTransaction(_2: Set[SingleTrajectory]): Array[String] = {
    var arr: Array[String] = Array[String]()
    _2.foreach(x=>{
      arr = arr :+ x.poi
    })
    arr
  }

  def getSequences(_2: Set[SingleTrajectory], mapPoiToInteger:mutable.Map[String, Int]): Array[Array[Int]] = {
    var arr: Array[Array[Int]] = Array[Array[Int]]()
    _2.foreach(x=>{
      arr = arr :+  Array[Int](mapPoiToInteger.get(x.poi).get)
    })
    arr
  }

  def computeTrajectoryUsingFPGrowth(df: DataFrame, stringShapeMap: mutable.Map[String, String], minSupport: Double = 0.01, minConfidence:Double = 0.2) = {
    val logger = Logger.getLogger("TrajectoryExtraction@computeTrajectoryUsingFPGrowth")

    val prepareTransaction = df
      .rdd
      .map(x=>mapCreateTrajectory(x,stringShapeMap))
      .reduceByKey((x,y)=> x ++ y)
      .filter(x=>x._2.size > 0)

    val transactions = prepareTransaction
      .map(x=>getTransaction(x._2))

    val fpg = new FPGrowth()
      .setMinSupport(minSupport)

    val model = fpg.run(transactions)

    (model.freqItemsets.collect(),model.generateAssociationRules(minConfidence).collect())
  }

  def generateFreqItemset(x: PrefixSpan.FreqSequence[Int], mapIntegerToPoi: mutable.Map[Int, String]): FreqItemset[String] = {
    val frequency = x.freq
    var arr : Array[String] = Array[String]()
    x.sequence.foreach(sequence => {
      sequence.foreach(singleSequence=>{
        arr = arr :+ mapIntegerToPoi.get(singleSequence).get
      })
    })
    new FreqItemset(arr,frequency)
  }

  def computeTrajectoryUsingPrefixSpan(df: DataFrame, stringShapeMap: mutable.Map[String, String], minSupport: Double = 0.01, maxPatternLength:Int = 5, minConfidence:Double = 0.01)  = {
    val logger = Logger.getLogger("TrajectoryExtraction@computeTrajectoryUsingPrefixSpan")
    val mapPoiToInteger = mutable.Map[String, Int]().withDefaultValue(0)
    val mapIntegerToPoi = mutable.Map[Int, String]()
    var count = 0
    stringShapeMap.foreach(x=>{
      mapPoiToInteger.update(x._1, count)
      mapIntegerToPoi.update(count,x._1)
      count = count + 1
    })

    val prepareTransaction = df
      .rdd
      .map(x => mapCreateTrajectory(x, stringShapeMap))
      .reduceByKey((x, y) => x ++ y)
      .filter(x => x._2.size > 0)

    val transactions = prepareTransaction
      .map(x => getSequences(x._2, mapPoiToInteger))
      .cache()

    val prefixSpan = new PrefixSpan()
      .setMinSupport(minSupport)
      .setMaxPatternLength(maxPatternLength)
    val model = prefixSpan.run(transactions)

    val ruleGeneration = model
      .freqSequences
      .map(x=>generateFreqItemset(x,mapIntegerToPoi))

    val ar = new AssociationRules()
      .setMinConfidence(minConfidence)
    val results = ar.run(ruleGeneration)
    logger.info("Done prefixspan")
    (ruleGeneration.collect(), results.collect())
    }



  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    var dataPath = scala.io.StdIn.readLine("Insert dataset's absoulute path or leave empty to use default one: ")
    if (dataPath.equalsIgnoreCase(""))
      dataPath = defaultDataset
    var pathToKml = scala.io.StdIn.readLine("Insert kml's absoulute path or leave empty to use default one: ")
    if (pathToKml.equalsIgnoreCase(""))
      pathToKml = kmlPath
    val rois : Array[(String,Geometry)] = readRois("")
    val stringShapeMap = KMLUtils.lookupFromKml(pathToKml).asScala
    val tests = Array[Int](8,16,32,64)
    var results = Seq[TestResults]()
    val gson = new Gson()
    val logger = Logger.getLogger("TrajectoryExtraction@Test")
    logger.info("Executing test for TrajectoryExtraction")
    tests.foreach(step=>{
      logger.info("Executing step %d".format(step))
      val tIn = System.currentTimeMillis()
      val sparkMasterAddress = "local[%d]".format(step)
      val spark = Factory.createSparkSession(appName, sparkMasterAddress)
      val df = DataTransform.filterFlickrDataframe(Factory.readJsonAsDataframe(dataPath,spark,appName))
      //Run algo
      val res = computeTrajectoryUsingPrefixSpan(df,stringShapeMap)
      val elapsedTime = System.currentTimeMillis() -tIn
      val singleResult = TestResults("%d".format(step), elapsedTime=elapsedTime.toDouble/1000)
      gson.toJson(singleResult)
      results = results :+ singleResult
      logger.info("Time elapsed for step %d is %d".format(step,elapsedTime))
      spark.close()
    })
    logger.info("Save results in json formats")
    var pathToSave = scala.io.StdIn.readLine("Insert path to save execution outputs: ")
    if (pathToSave.equalsIgnoreCase(""))
      pathToSave = "./trajectory-executions-time.json"
    val saveExecutions = gson.toJson(results.toList.asJava)
    new PrintWriter(pathToSave) { write(saveExecutions); close }
    var resultsFP = Seq[TestResults]()
    tests.foreach(step=>{
      logger.info("Executing step %d".format(step))
      val tIn = System.currentTimeMillis()
      val sparkMasterAddress = "local[%d]".format(step)
      val spark = Factory.createSparkSession(appName, sparkMasterAddress)
      val df = DataTransform.filterFlickrDataframe(Factory.readJsonAsDataframe(dataPath,spark,appName))
      //Run algo
      val res = computeTrajectoryUsingFPGrowth(df,stringShapeMap)
      val elapsedTime = System.currentTimeMillis() -tIn
      val singleResult = TestResults("%d".format(step), elapsedTime=elapsedTime.toDouble/1000)
      gson.toJson(singleResult)
      resultsFP = resultsFP :+ singleResult
      logger.info("Time elapsed for step %d is %d".format(step,elapsedTime))
      spark.close()
    })
    logger.info("Save results in json formats")
    var pathToSaveFP = scala.io.StdIn.readLine("Insert path to save execution outputs: ")
    if (pathToSaveFP.equalsIgnoreCase(""))
      pathToSaveFP = "./trajectory-fp-executions-time.json"
    val saveExecutionsFP = gson.toJson(resultsFP.toList.asJava)
    new PrintWriter(pathToSaveFP) { write(saveExecutionsFP); close }


  }

}

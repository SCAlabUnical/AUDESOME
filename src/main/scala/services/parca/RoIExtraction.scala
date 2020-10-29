package services.parca

import java.io.PrintWriter

import util.TestResults
import java.util

import factory.Factory
import com.google.gson.Gson
import com.spatial4j.core.context.SpatialContext
import com.vividsolutions.jts.geom.Geometry
import dataServices.transform.DataTransform
import distance.KDistUtility
import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.Row
import workflow.utils.{ClusterPoint, GeometryUtils}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.Random

object RoIExtraction {

  var lookupTag: Map[String, Int] = Map()
  var current_sampling_rate = 5000
  val numNeighbours: Int = 4
  val appName = "RoIExtraction"
  val defaultDataset: String = "/home/emanuele/Documents/Tesi/FlickrRome2017.json"
  val defaultStopWord: String = "/home/emanuele/IdeaProjects/SparkTest/input/services.keywords/keywordslcurve.txt"
  var nexInt:Int = 0

  val createFromRdd: (Row, Seq[String]) => Array[Tuple2[Int, ClusterPoint]] = {
    (x, y) => {
      var returnStructure = Array[Tuple2[Int, ClusterPoint]]()
      val tags = x.getAs[mutable.WrappedArray[String]](5)
      tags.foreach(elem => {
        if (y.contains(elem)) {
          returnStructure = returnStructure :+ Tuple2(lookupTag(elem), new ClusterPoint(x.getDouble(1), x.getDouble(0), SpatialContext.GEO))
        }
      })
      returnStructure
    }
  }

  def sampling(x: (Int, Iterable[ClusterPoint])): (Int, Iterable[ClusterPoint]) = {
    if (x._2.size > current_sampling_rate) {
      return (x._1, Random.shuffle(x._2).take(current_sampling_rate))
    }
    (x._1, x._2)
  }

  def calculateEpsWithThreshold(x: (Int, Iterable[ClusterPoint])): (Int, Iterable[ClusterPoint], Double) = {
    val distances = KDistUtility.calculateKNN(x._2.toList.asJava, numNeighbours);
    val eps = KDistUtility.calculateEpsWithThreshold("", distances, 0.27)
    (x._1, x._2, eps)
  }

  def applyRoIWithEps: ((Int, util.List[ClusterPoint], Double) => (String, Geometry)) = {
    (x, y, eps) => {
      val tIn = System.currentTimeMillis()
      val geom = GeometryUtils.getRoI(y, eps, numNeighbours)
      if (System.currentTimeMillis() - tIn > 5000) {
        println(x)
        println(y.size())
        println("Elapsed time in apply roi: " + (System.currentTimeMillis() - tIn) + "ns")
      }
      (lookupTag.find(_._2 == x).get._1, geom)
    }
  }


  def automaticEpsDbcan(keywords: Seq[String], dataframe: org.apache.spark.sql.DataFrame, partitions: Int) = {
    val tIn = System.currentTimeMillis()
    var selectedNumberOfPartitions = 50
    if (partitions == 1) {
      selectedNumberOfPartitions = partitions;
    }
    else {
      selectedNumberOfPartitions = keywords.size
    }
    keywords.foreach(x => {
      if (!lookupTag.contains(x)) {
        lookupTag += (x -> nexInt)
        nexInt += 1
      }
    })
    //Grouping by poi and collect points
    val data = dataframe
      .rdd
      .flatMap(x => createFromRdd(x, keywords))
      .groupByKey()
      .filter(x => x._2.size > 50)
      .map(x => sampling(x))

    //Partition data using HashPartitioning on integer columns. Using data.count() we assign one partition to a single task
    val partitionedData = data
    //.partitionBy(new HashPartitioner(selectedNumberOfPartitions))
    //compute eps for services.keywords
    val epsComputed = partitionedData.map(x => calculateEpsWithThreshold(x))
    //collect result
    val res = epsComputed
      .mapPartitions(iter => {
        iter.map(x => applyRoIWithEps(x._1, x._2.toList.asJava, x._3)).filter(x => x._2 != null)
      })
      .collect()
    println("Elapsed time: " + (System.currentTimeMillis() - tIn) + "ns")
    res
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    var dataPath = scala.io.StdIn.readLine("Insert dataset's absoulute path or leave empty to use default one: ")
    if (dataPath.equalsIgnoreCase(""))
      dataPath = defaultDataset
    var stopWord = scala.io.StdIn.readLine("Insert precomputed services.keywords: ")
    if (stopWord.equalsIgnoreCase(""))
      stopWord = defaultStopWord
    print("Insert number of fixed point (default = 5000): ")
    var numberOfFixedPoitn = scala.io.StdIn.readInt()
    if (numberOfFixedPoitn > 0)
      current_sampling_rate = numberOfFixedPoitn
    val tests = Array[Int](8, 16, 32, 64)
    var results = Seq[TestResults]()
    val gson = new Gson()
    val logger = Logger.getLogger("RoIExtraction@Test")
    logger.info("Executing test for RoIExtraction")
    tests.foreach(step => {
      logger.info("Executing step %d".format(step))
      val tIn = System.currentTimeMillis()
      val keywords = Source.fromFile(stopWord).getLines.toSet
      keywords.foreach(x => {
        if (!lookupTag.contains(x)) {
          lookupTag += (x -> nexInt)
          nexInt += 1
        }
      })
      val sparkMasterAddress = "local[%d]".format(step)
      val spark = Factory.createSparkSession(appName, sparkMasterAddress)
      val df = DataTransform.filterFlickrDataframe(Factory.readJsonAsDataframe(dataPath, spark, appName))
      automaticEpsDbcan(keywords = keywords.toSeq, dataframe = df, partitions = step)
      val elapsedTime = System.currentTimeMillis() - tIn
      val singleResult = TestResults("%d".format(step), elapsedTime = elapsedTime.toDouble / 1000)
      gson.toJson(singleResult)
      results = results :+ singleResult
      logger.info("Time elapsed for step %d is %d".format(step, elapsedTime))
      spark.close()
    })
    logger.info("Save results in json formats")
    var pathToSave = scala.io.StdIn.readLine("Insert path to save execution outputs: ")
    if (pathToSave.equalsIgnoreCase(""))
      pathToSave = "./roi-executions-time.json"
    val saveExecutions = gson.toJson(results.toList.asJava)
    new PrintWriter(pathToSave) {
      write(saveExecutions);
      close
    }

  }

}

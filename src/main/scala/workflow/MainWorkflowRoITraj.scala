package workflow

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.sql.Timestamp
import java.util

import factory.Factory
import com.spatial4j.core.context.SpatialContext
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import dataServices.transform.DataTransform
import distance.KDistUtility
import services.keywords.KeywordsExtraction
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.ml.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{array, array_distinct, col, collect_list, date_format, explode, to_timestamp, udf}
import trajectory.util.KMLUtils
import workflow.utils.{ClusterPoint, GeometryUtils}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io
import scala.util.Random

object MainWorkflowRoITraj {

  val appName = "MainWorkflowRoITraj-ScalaWorkflow"
  val defaultDataset: String = "/home/emanuele/Documents/Tesi/FlickrRome2017-25.json"
  val keywordsTest: String = "/home/emanuele/IdeaProjects/SparkTest/input/services.keywords/test.txt"
  val defaultStopWord: String = "/home/emanuele/IdeaProjects/SparkTest/src/main/scala/script/new_stop_words.txt"
  val realShapesPath: String = "/home/emanuele/IdeaProjects/SparkTest/input/kml/Rome/Real shapes v2.kml"
  ///dbscan parameters TODO: use properties and run configuration
  val eps: Double = 50
  val numNeighbours: Int = 4
  var nexInt:Int = 0
  var current_sampling_rate = 5000
  val random = new Random()

  var lookupTag:Map[String,Int] = Map()

  def readFile(filename: String): Seq[String] = {
    val bufferedSource = io.Source.fromFile(filename)
    val lines = (for (line <- bufferedSource.getLines()) yield line).toList
    bufferedSource.close
    lines
  }

  val createCellFromArray: (util.List[mutable.WrappedArray[Double]] => mutable.Buffer[ClusterPoint]) = {
    x => {
      val returnList = mutable.Buffer[ClusterPoint]()
      for (i <- 0 until x.size() - 1) {
        val elem = x.get(i)
        if (elem.length > 1) {
          val cell = new ClusterPoint(elem(1), elem(0), SpatialContext.GEO)
          returnList.append(cell)
        }
      }
      returnList
    }
  }

  def applyRoI: ((Int, util.List[ClusterPoint]) => (String, Geometry)) = {
    (x, y) => {
      val tIn = System.currentTimeMillis()
      val geom = GeometryUtils.getRoI(y, eps, numNeighbours)
      if (System.currentTimeMillis()-tIn > 5000){
        println(x)
        println(y.size())
        println("Elapsed time in apply roi: " + (System.currentTimeMillis() - tIn) + "ns")
      }
      (lookupTag.find(_._2==x).get._1, geom)
    }
  }

  def applyRoIWithEps: ((Int, util.List[ClusterPoint], Double) => (String, Geometry)) = {
    (x, y, eps) => {
      val tIn = System.currentTimeMillis()
      val geom = GeometryUtils.getRoI(y, eps, numNeighbours)
      if (System.currentTimeMillis()-tIn > 5000){
        println(x)
        println(y.size())
        println("Elapsed time in apply roi: " + (System.currentTimeMillis() - tIn) + "ns")
      }
      (lookupTag.find(_._2==x).get._1, geom)
    }
  }


  //Define udf for lookup roi of a given point
  def getPlacesFromLocation(x:Array[Tuple2[String,Geometry]]) = udf((lat: Double, lon: Double) => {
    val gf = new GeometryFactory
    val point = gf.createPoint(new Coordinate(lon, lat))
    var ret = "-1"
    for ( tuple <- x ) {
      if (tuple._2.contains(point))
        ret = tuple._1
    }
    ret
  })

  implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }

  val sortDatesAndGetPlace = udf { arr: mutable.WrappedArray[GenericRowWithSchema] =>
    arr.sortBy(_.getAs[Timestamp](1)).map { case Row(i: String, s: Timestamp, j: String) => (i) }
  }

//  def dbscanRoIWrapper(services.keywords: Seq[String], dataframe: org.apache.spark.sql.DataFrame): Array[(String, Geometry)] = {
//    val tIn = System.currentTimeMillis()
//    val columns = dataframe.columns.map(col) :+ (explode(col("value")) as "explodedTags")
//    //Grouping by poi and collect points
//    val unfoldedTags = dataframe.select(columns: _*)
//      .filter(col("explodedTags").isin(services.keywords: _*))
//      .groupBy("explodedTags")
//      .agg(collect_list(array("latitude", "longitude")) as "ClusterPointList")
//      .select("explodedTags", "ClusterPointList")
//      .repartition(col("explodedTags"))
//      .rdd
//      .map(x => (x.getString(0), createCellFromArray(x.getList[mutable.WrappedArray[Double]](1))))
//    //TODO: Apply RoI algorithms is written in java.
//    val rois = unfoldedTags.map(x => applyRoI(x._1, x._2.asJava)).filter(x => x._2 != null).collect()
//    println("Elapsed time: " + (System.currentTimeMillis() - tIn) + "ns")
//    rois
//  }

  val contains: (mutable.WrappedArray[String], Seq[String]) => Boolean = {
    (x,y) => {
      x.foreach(elem=> {
        if (y.contains(elem))
          true
      })
      false
    }
  }

  val createFromRdd: (Row, Seq[String]) => Array[Tuple2[Int,ClusterPoint]] = {
    (x, y) => {
      var returnStructure = Array[Tuple2[Int,ClusterPoint]]()
      val tags = x.getAs[mutable.WrappedArray[String]](5)
      tags.foreach(elem=>{
        if (y.contains(elem)){
          returnStructure = returnStructure :+ Tuple2(lookupTag.get(elem).get,new ClusterPoint(x.getDouble(1),x.getDouble(0), SpatialContext.GEO))
        }
      })
      returnStructure
    }
  }


  def filtered(x: (String, Iterable[ClusterPoint])): (String, Iterable[ClusterPoint]) = {
    (x._1,x._2.toSet)
  }


  def sampling(x: (Int, Iterable[ClusterPoint])): (Int,Iterable[ClusterPoint]) = {
    if (x._2.size > current_sampling_rate){
      return (x._1, Random.shuffle(x._2).take(current_sampling_rate))
    }
    (x._1,x._2)
  }



  def dbscanRoIWrapperRdd(keywords:Seq[String], dataframe:org.apache.spark.sql.DataFrame, partitions: Int) ={
    val tIn = System.currentTimeMillis()
    var selectedNumberOfPartitions = 50
    if (partitions > 0){
      selectedNumberOfPartitions = partitions;
    }
    //Grouping by poi and collect points
    val data = dataframe
      .rdd
      .flatMap(x=> createFromRdd(x,keywords))
      .groupByKey()
      .filter(x => x._2.size > 50)
      .map(x=>sampling(x))
    val count = selectedNumberOfPartitions
    //Partition data using HashPartitioning on integer columns. Using data.count() we assign one partition to a single task
    val partitionedData = data
      .partitionBy(new HashPartitioner(count.toInt))
    //collect result
    val res = partitionedData
      .mapPartitions(iter => {iter.map(x=> applyRoI(x._1,x._2.toList.asJava)).filter(x=>x._2!=null)})
      .collect()
    println("Elapsed time: " + (System.currentTimeMillis() - tIn) + "ns")
    res
  }

  def calculateEpsWithThreshold(x: (Int, Iterable[ClusterPoint])): (Int, Iterable[ClusterPoint], Double) = {
    val distances = KDistUtility.calculateKNN(x._2.toList.asJava,numNeighbours);
    val eps = KDistUtility.calculateEpsWithThreshold("",distances,0.27)
    (x._1,x._2,eps)
  }

  def automaticEpsDbcan(keywords:Seq[String], dataframe:org.apache.spark.sql.DataFrame, partitions: Int) = {
    val tIn = System.currentTimeMillis()
    var selectedNumberOfPartitions = 50
    if (partitions == 1) {
      selectedNumberOfPartitions = partitions;
    }
    else{
      selectedNumberOfPartitions = keywords.size
    }
    //Grouping by poi and collect points
    val data = dataframe
      .rdd
      .flatMap(x => createFromRdd(x, keywords))
      .groupByKey()
      .filter(x => x._2.size > 50)
      .map(x => sampling(x))

    //Partition data using HashPartitioning on integer columns. Using data.count() we assign one partition to a single task
    val partitionedData = data
      .partitionBy(new HashPartitioner(selectedNumberOfPartitions))
    //compute eps for services.keywords
    val epsComputed = partitionedData.map(x => calculateEpsWithThreshold(x))
    //collect result
    val res = epsComputed
      .mapPartitions(iter => {
        iter.map(x => applyRoIWithEps(x._1, x._2.toList.asJava, x._3)).filter(x => x._2 != null)
      })
      .collect()
//    res.foreach(x => {
//      val writer = new PrintWriter(new File("./shapes_dbscan_auto/%s.txt".format(x._1)))
//      writer.write(x._2.toString)
//      writer.close()
//    })
    println("Elapsed time: " + (System.currentTimeMillis() - tIn) + "ns")
    res
  }

  def fpGrowthWrapper(rois:Array[(String,Geometry)], dataframe:org.apache.spark.sql.DataFrame, keywords: Seq[String]): Tuple2[FPGrowthModel,org.apache.spark.sql.DataFrame] = {
    val tIn = System.currentTimeMillis()
    val columns = dataframe.columns.map(col) :+ (explode(col("value")) as "explodedTags")
    val windowedDF = dataframe
      .select(columns: _*)
      .filter(col("explodedTags").isin(keywords:_*))
      .withColumn("daytime", date_format(to_timestamp(col("datePosted"), "MMM d, yyyy hh:mm:ss a"), "MMM d, yyyy"))
      .withColumn("full_timestamp", to_timestamp(col("datePosted"), "MMM d, yyyy hh:mm:ss a"))
      .withColumn("location", getPlacesFromLocation(rois)(col("latitude"), col("longitude")))
      .filter(!(col("location") === "-1"))
      .groupBy("username", "daytime")
      .agg(collect_list(col("location")) as "payload")
      //TODO:fix sortandgetplaces
      .select(col("username"), col("daytime"), array_distinct(col("payload")) as "trajectory_set")
    print("Insert minSupport: ")
    val minSupport = scala.io.StdIn.readDouble()
    print("Insert minConfidence: ")
    val minConfidence = scala.io.StdIn.readDouble()
    val fpgrowth = new FPGrowth()
      .setItemsCol("trajectory_set")
      .setMinSupport(minSupport)
      .setMinConfidence(minConfidence)
    println("Computing FPGrowth model")
    val model = fpgrowth.fit(windowedDF)
    val spmResult = model.transform(windowedDF)
    model.freqItemsets.show(false)
    model.associationRules.show(false)
    spmResult.show(false)
    println("Elapsed time: " + (System.currentTimeMillis() - tIn) + "ns")
    (model,spmResult)
  }


  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)
    var sparkMasterAddress = scala.io.StdIn.readLine("Insert spark's master address or leave empty to use local spark execution: ")
    if (sparkMasterAddress.equalsIgnoreCase(""))
      sparkMasterAddress = "local[*]"
    val spark = Factory.createSparkSession(appName, sparkMasterAddress)
    var dataPath = scala.io.StdIn.readLine("Insert dataset's absoulute path or leave empty to use default one: ")
    if (dataPath.equalsIgnoreCase(""))
      dataPath = defaultDataset
    val df = DataTransform.filterFlickrDataframe(Factory.readJsonAsDataframe(dataPath,spark,appName))
    println("Number of partitions is %s".format(df.rdd.getNumPartitions))
    println("Keywords section")
    print("Using precomputed services.keywords flag: ")
    val precomputedKeywords = scala.io.StdIn.readBoolean()
    var keywords = Seq[String]()
    if (precomputedKeywords) {
      var pathToKeywords = scala.io.StdIn.readLine("Insert absolute path to services.keywords: ")
      if (pathToKeywords.equals("")){
        pathToKeywords = keywordsTest
      }
      keywords = readFile(pathToKeywords)
    }
    else{
      //TODO: insert switch case and tfidf
      var stopWord = scala.io.StdIn.readLine("Insert precomputed stop word's set: ")
      if (stopWord.equalsIgnoreCase(""))
        stopWord = defaultStopWord
      var algoType = scala.io.StdIn.readLine("Insert algorithm (TF-IDF, LCurve): ")
      if (algoType.equalsIgnoreCase("")){
        algoType = "TF-IDF"
      }
      if (algoType.toUpperCase().equalsIgnoreCase("TF-IDF")){
        println("Use algorithm TF-IDF")
        keywords = KeywordsExtraction.computeRddTfIdfLogScaled(dataPath,spark,stopWord,200).toSeq
        //services.keywords = KeywordsTFIDFSpark.tfidfKeywordsAlgo(df,stopWord)
      }
      else if(algoType.toUpperCase.equals("LCURVE")){
        println("Use algorithm lcurve")
        keywords = KeywordsExtraction.computeRddLcurve(dataPath,spark,stopWord,200)._1.toSeq
        //services.keywords = KeywordsLCurveSpark.lcurvefKeywordsAlgo(df,stopWord)
      }
      else{
        println("Use default algorithms TF-IDF")
        keywords = KeywordsExtraction.computeRddTfIdfLogScaled(dataPath,spark,stopWord,200).toSeq
        //services.keywords = KeywordsTFIDFSpark.tfidfKeywordsAlgo(df,stopWord)
      }
    }
    keywords.foreach(x=>{
      if (!lookupTag.contains(x)){
        lookupTag += (x -> nexInt)
        nexInt += 1
      }
    })
    println("Done services.keywords section")
    println("RoI Extraction section")
    //val stringShapeMap = KMLUtils.lookupFromKml(realShapesPath).asScala
    var rois:Array[(String,Geometry)] = null
    print("Insert the number of partition: ")
    val nPartitions = scala.io.StdIn.readInt()
    print("Insert number of relevants point to be sampled: ")
    var samplingRate = scala.io.StdIn.readInt()
    var clusterAlgo = scala.io.StdIn.readLine("Insert algorithm (Dbscan): ")
    if (samplingRate > 0)
      current_sampling_rate = samplingRate
    if (clusterAlgo.equalsIgnoreCase("")){
      clusterAlgo = "DBSCAN"
    }
    if (clusterAlgo.toUpperCase.equalsIgnoreCase("DBSCAN")){
      println("Computing roi using DBSCAN")
      //dbscanRoIWrapperRdd(services.keywords,df)
      //rois = dbscanRoIWrapper(services.keywords,df)
      //rois = dbscanRoIWrapperRdd(services.keywords,df,nPartitions)
      rois = automaticEpsDbcan(keywords,df,nPartitions)
    }
    else{
      println("Using default clustering algorithms")
      rois = dbscanRoIWrapperRdd(keywords,df,nPartitions)
    }
    println("Done RoI Extraction section")
    println("Trajectory Extraction section")
    var fpmAlgo:String = scala.io.StdIn.readLine("Insert algorithm (FPGrowth): ")
    var trajectoryOutput: (FPGrowthModel,org.apache.spark.sql.DataFrame) = null
    if (fpmAlgo.equalsIgnoreCase("")){
      fpmAlgo = "FPGROWTH"
    }
    if (fpmAlgo.toUpperCase.equalsIgnoreCase("FPGROWTH")){
      println("Computing trajectory using FPGROWTH")
      trajectoryOutput = fpGrowthWrapper(rois,df,keywords)
    }
    else{
      println("Using default services.fpm algorithms")
      trajectoryOutput = fpGrowthWrapper(rois,df,keywords)
    }
    println("Done Trajectory Extraction section")

  }

}

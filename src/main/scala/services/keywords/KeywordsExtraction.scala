package services.keywords

import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable
import dataServices.dao.{FlickrParquetDao,FlickrJsonDao}
import dataServices.transform.DataTransform
import scala.io.Source

object KeywordsExtraction {

  val appName = "KeywordsExtraction"
  val defaultDataset: String = "/home/emanuele/Documents/Tesi/FlickrRome2017-25.json"
  val defaultStopWord: String = "/home/emanuele/IdeaProjects/SparkTest/src/main/scala/script/new_stop_words.txt"
  val minOcc = 100
  val step1mY = 8.992909382672273E-6
  val step1mX = 1.2080663828690774E-5
  val minDocumentFrequency = 2
  val maxDocumentFrequency = 10
  val topN = 10
  val globalElbowFunction: (Array[Int]) => Array[Double] = {
    x => DataTransform.getElbowPoint(x)
  }
  val globalElbowFunctionScore: (mutable.WrappedArray[Double]) => Double = {
    x => DataTransform.getElbowPointScore(x.toArray)(3)
  }

  def containsHanScript(s: String): Boolean = s.codePoints.anyMatch((codepoint: Int) => Character.UnicodeScript.of(codepoint) eq Character.UnicodeScript.HAN)

  def containsNoSpecialChars(string: String) = string.matches("^[a-zA-Z0-9]*$")

  def computeRawTermFrequency(x: Row, stopWord: Set[String]): Array[Tuple2[String, Int]] = {
    val tags = x.getAs[mutable.WrappedArray[String]](2)
    val tempMap = collection.mutable.Map[String, Int]().withDefaultValue(0)
    tags.foreach(tag => {
      if (!stopWord.contains(tag))
        tempMap.update(tag, tempMap(tag) + 1)
    })
    tempMap.toArray
  }

  def computeDocumentFrequency(x: Row, stopWord: Set[String]): Array[Tuple2[String, Int]] = {
    val tags: mutable.WrappedArray[String] = x.getAs[mutable.WrappedArray[String]](2)
    val tempMap = collection.mutable.Map[String, Int]()
    tags.foreach(tag => {
      if (!stopWord.contains(tag))
        tempMap.put(tag, 1)
    })
    tempMap.toArray
  }

  def logNormalizationTF(x: (String, Int)): Tuple2[String, Double] = {
    (x._1, 1 + Math.log(x._2))
  }

  def computeAdjustedTermFrequency(x: (String, Int), count: Int): Tuple2[String, Double] = {
    (x._1, x._2.toDouble / count.toDouble)
  }

  def createCell(x: Row, cellSize: Int): Cell = {
    val stepX = cellSize * step1mX
    val stepY = cellSize * step1mY
    val lat = x.getDouble(0)
    val lon = x.getDouble(1)
    val keyLatitude = ((lat / stepY).toInt + 1) * stepY - 0.5 * stepY
    val keyLongitude = ((lon / stepX).toInt + 1) * stepX - 0.5 * stepX
    val cell = new Cell(lat = keyLatitude, lon = keyLongitude)
    cell
  }

  def computeTermFrequency(x: Row, stopWord: Set[String]): mutable.Map[String, Int] = {
    val tags: mutable.WrappedArray[String] = x.getAs[mutable.WrappedArray[String]](5)
    val tempMap = mutable.Map[String, Int]().withDefaultValue(0)
    tags.foreach(tag => {
      if (!stopWord.contains(tag) && !containsHanScript(tag) && !tag.exists(_.isDigit) && containsNoSpecialChars(tag))
        tempMap.update(tag, tempMap(tag) + 1)
    })
    tempMap
  }

  def dfMapperFunction(x: Row, y: Cell, stopWord: Set[String]): Array[Tuple2[String, Int]] = {
    val tags: mutable.WrappedArray[String] = x.getAs[mutable.WrappedArray[String]](2)
    val tempMap = collection.mutable.Map[String, Int]()
    tags.foreach(tag => {
      if (!stopWord.contains(tag))
        tempMap.put(tag, 1)
    })
    tempMap.toArray
  }

  def reducerFunction(map1: mutable.Map[String, Int], map2: mutable.Map[String, Int]): mutable.Map[String, Int] = {
    val merged = (map1.keySet ++ map2.keySet).map { i => (i, map1.getOrElse(i, 0) + map2.getOrElse(i, 0)) }.toMap
    val returnMap = mutable.Map() ++ merged
    returnMap
  }

  def localFilterFunction(x: mutable.Map[String, Int], minOccurence: Int, documentFrequencyMap: Map[String, Double]): mutable.Map[String, Double] = {
    val resultMap = mutable.Map[String, Double]()
    x.filter(_._2 >= minOccurence).foreach(entry => {
      if (documentFrequencyMap.contains(entry._1)) {
        resultMap.update(entry._1, (1 + Math.log(entry._2)) * documentFrequencyMap.get(entry._1).get)
      }
    })
    resultMap
  }

  def reducerArrayFunction(x: mutable.WrappedArray[String], y: mutable.WrappedArray[String]): Array[String] = {
    val merge = x ++ y
    merge.toSet.toArray
  }

  def flatMapSplitFunction(tags: mutable.WrappedArray[String], stopWord: Set[String]): Array[Tuple2[String, Int]] = {
    val tempMap = collection.mutable.Map[String, Int]()
    tags.foreach(tag => {
      if (!stopWord.contains(tag) && !containsHanScript(tag) && !tag.exists(_.isDigit) && containsNoSpecialChars(tag))
        tempMap.put(tag, 1)
    })
    tempMap.toArray
  }

  def topN(x: (mutable.Map[String, Double]), n: Int): Seq[Keywords] = {
    x
      .toSeq
      .sortBy(_._2)(Ordering[Double].reverse)
      .map(x => new Keywords(term = x._1, score = x._2))
      .take(n)
  }

  def computeRddTfIdfLogScaled(dataPath: String, spark: SparkSession, stopWordPath: String, cellSize: Int): Set[String] = {
    /**
     * Given a Flickr's data path and spark session return set of services.keywords from
     */
    val df = FlickrJsonDao(spark).readData(path = dataPath) //DataTransform.filterFlickrDataframe(Factory.readJsonAsDataframe(dataPath, spark, "computeRddTfIdfLogScaled"))
    val stopWordSet = Source.fromFile(stopWordPath).getLines.toSet

    /**
     * Document frequency section
     */

    /**
     * TODO: Talk about this. We need document frequency, and collect as Map
     */
    val cellRdd = df
      .rdd
      .map(x => (createCell(x, cellSize), x.getAs[mutable.WrappedArray[String]](5)))
      .reduceByKey((x, y) => reducerArrayFunction(x, y))
      .filter(x => x._2.length > 0)

    val documentLenght: Int = cellRdd.count.toInt

    val documentFrequencyMap = cellRdd
      .flatMap(x => flatMapSplitFunction(x._2, stopWordSet))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._1, Math.log(documentLenght / x._2.toDouble)))
      .collectAsMap()
      .toMap
      .filter(x => x._2 >= minDocumentFrequency && x._2 <= maxDocumentFrequency)

    /**
     * Compute term frequency per cell
     */
    val tfIDFPerCell = df
      .rdd
      .map(x => (createCell(x, cellSize), computeTermFrequency(x, stopWordSet)))
      .reduceByKey((map1, map2) => reducerFunction(map1, map2))
      .map(x => (x._1, localFilterFunction(x._2, minOcc, documentFrequencyMap)))
      .filter(x => x._2.nonEmpty)
      .mapPartitions(iter => {
        iter.map(x => topN(x._2, topN))
      })
      .collect()
      .toSeq.flatten.toSet

    var returnKeywords = Array[String]()
    tfIDFPerCell.foreach(keyword => {
      returnKeywords = returnKeywords :+ keyword.term
    })
    returnKeywords.toSet
  }

  def getValuesAsArray(_2: mutable.Map[String, Int]): Seq[Int] = {
    _2
      .toSeq
      .map(x => x._2)
  }

  def localLCurveFilterFunction(x: mutable.Map[String, Int], minOccurence: Int, documentFrequencyMap: Map[String, Double]): mutable.Map[String, Double] = {
    val resultMap = mutable.Map[String, Double]()
    x.filter(_._2 > minOccurence).foreach(entry => {
      if (documentFrequencyMap.contains(entry._1)) {
        resultMap.update(entry._1, (1 + Math.log(entry._2)) * documentFrequencyMap.get(entry._1).get)
      }
    })
    val localPoints = resultMap.toSeq.map(x => x._2).sorted(Ordering[Double].reverse)
    val cutPoint = globalElbowFunctionScore(localPoints.toArray)
    resultMap.filter(_._2 >= cutPoint)
  }

  def getAll(x: mutable.Map[String, Double]): Seq[Keywords] = {
    x
      .toSeq
      .sortBy(_._2)(Ordering[Double].reverse)
      .map(x => new Keywords(term = x._1, score = x._2))
  }

  def computeRddLcurve(dataPath: String, spark: SparkSession, stopWordPath: String, cellSize: Int): Set[String] = {
    val df = FlickrParquetDao(spark).readData(dataPath) //DataTransform.filterFlickrDataframe(Factory.readJsonAsDataframe(dataPath, spark, appName))
    val stopWordSet = Source.fromFile(stopWordPath).getLines.toSet

    /**
     * TODO: Talk about this. We need document frequency, and collect as Map
     */
    val cellRdd = df
      .rdd
      .map(x => (createCell(x, cellSize), x.getAs[mutable.WrappedArray[String]](5)))
      .reduceByKey((x, y) => reducerArrayFunction(x, y))
      .filter(x => x._2.length > 0)

    val documentLenght: Int = cellRdd.count.toInt

    val documentFrequencyMap = cellRdd
      .flatMap(x => flatMapSplitFunction(x._2, stopWordSet))
      .reduceByKey((x, y) => x + y)
      .filter(x => x._2 > minDocumentFrequency)
      .map(x => (x._1, Math.log(documentLenght / x._2.toDouble)))
      .collectAsMap()
      .toMap

    val tfIDFPerCell = df
      .rdd
      .map(x => (createCell(x, cellSize), computeTermFrequency(x, stopWordSet)))
      .reduceByKey((map1, map2) => reducerFunction(map1, map2))
    /**
     * compute the first global elbow
     */

    val globalCutPoints = tfIDFPerCell.map(x => (x._1, getValuesAsArray(x._2)))
      .reduceByKey((x, y) => (x ++ y))
      .map(x => x._2)
      .collect
      .flatten
      .sorted(Ordering.Int.reverse)
    val globalElbow: Double = globalElbowFunction(globalCutPoints)(3)

    val scorePerCell = tfIDFPerCell
      .map(x => (x._1, localLCurveFilterFunction(x._2, globalElbow.toInt, documentFrequencyMap)))
      .filter(x => x._2.nonEmpty)

    val finalResults = scorePerCell
      .mapPartitions(iter => {
        iter.map(x => getAll(x._2))
      }).collect
      .toSeq
      .flatten
      .toSet

    var returnKeywords = Array[String]()
    finalResults.foreach(keyword => {
      returnKeywords = returnKeywords :+ keyword.term
    })
    returnKeywords.toSet
  }


}

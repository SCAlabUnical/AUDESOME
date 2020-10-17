package dataServices.transform

import org.apache.spark.sql.functions.{col, collect_set, size, udf}
import org.apache.spark.sql.DataFrame

object DataTransform {

  def filterFlickrDataframe(dataframe: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    try {
      dataframe
        .select("geoData.latitude", "geoData.longitude", "geoData.accuracy", "owner.username", "datePosted", "tags.value")
        .filter(col("latitude") > 0 && col("longitude") > 0)
        .where(size(col("value")) > 0)
    }catch {
      case x: Exception =>{
        dataframe
          .select("EXTRA.geoData.latitude", "EXTRA.geoData.longitude", "EXTRA.geoData.accuracy", "EXTRA.owner.username", "EXTRA.datePosted", "EXTRA.tags.value")
          .filter(col("latitude") > 0 && col("longitude") > 0)
          .where(size(col("value")) > 0)
      }
    }
  }

  def groupingFlickrPerCell(dataframe:DataFrame, stepX:Double, stepY:Double, colMerge:String):DataFrame = {
    //defining some udf
    val createKeyLatitude = (latitude: Double) => {
      ((latitude / stepY).toInt + 1) * stepY - 0.5 * stepY
    }

    val createKeyLongitude = (longitude: Double) => {
      ((longitude / stepX).toInt + 1) * stepX - 0.5 * stepX
    }

    val keyLatitude = udf(createKeyLatitude)
    val keyLongitude = udf(createKeyLongitude)

    //create cell using udf
    val cellDataframe = dataframe
      .withColumn("keyLatitude", keyLatitude(col("latitude")))
      .withColumn("keyLongitude", keyLongitude(col("longitude")))

    val mergeUDAF = new MergeListsUDAF()

    val groupedPerCell = cellDataframe.groupBy("keyLatitude", "keyLongitude")
      .agg(mergeUDAF(col(colMerge)))
      //.repartition(col("keyLatitude"), col("keyLongitude"))

    groupedPerCell
  }

  def getElbowPoint2(distances: Array[Long]) = {
    val A = 0
    val B = distances.length - 1
    if (B - A < 2) throw new RuntimeException("Tra A e B ci deve almeno essere un elemento")
    val dim = B - A + 1
    // max variables
    var iMax = 0
    var distMax: Double = 0
    var xMax: Double = 0
    var yMax: Double = 0
    // temporary variables
    var xNorm: Double = -1
    var yNorm: Double = -1
    var tmpDist: Double = 0
    val yB = distances(B)
    val yA = distances(A)
    var yI = 0.0d
    val xB = B
    val xA = A
    var xI = 0.0d
    var i = 0
    val it = distances.iterator
    while ( {
      it.hasNext
    }) {
      yI = it.next
      if (i == A) {
        distMax = 0
        iMax = i
        xMax = i
        yMax = yI
      }
      else if (i > A && i <= B) {
        xI = i
        xNorm = (xI - xA) / (xB - xA)
        yNorm = (yI - yB) / (yA - yB)
        tmpDist = ((1.0 - yNorm) - xNorm) * Math.sqrt(2.0) / 2.0
        if (tmpDist > distMax) {
          distMax = tmpDist
          iMax = i
          xMax = xI
          yMax = yI
        }
      }
      i += 1
    }
    Array[Double](iMax, distMax, xMax, yMax)
  }

  def getElbowPoint(distances: Array[Int]) = {
    val A = 0
    val B = distances.length - 1
    if (B - A < 2) throw new RuntimeException("Tra A e B ci deve almeno essere un elemento")
    val dim = B - A + 1
    // max variables
    var iMax = 0
    var distMax: Double = 0
    var xMax: Double = 0
    var yMax: Double = 0
    // temporary variables
    var xNorm: Double = -1
    var yNorm: Double = -1
    var tmpDist: Double = 0
    val yB = distances(B)
    val yA = distances(A)
    var yI = 0.0d
    val xB = B
    val xA = A
    var xI = 0.0d
    var i = 0
    val it = distances.iterator
    while ( {
      it.hasNext
    }) {
      yI = it.next
      if (i == A) {
        distMax = 0
        iMax = i
        xMax = i
        yMax = yI
      }
      else if (i > A && i <= B) {
        xI = i
        xNorm = (xI - xA) / (xB - xA)
        yNorm = (yI - yB) / (yA - yB)
        tmpDist = ((1.0 - yNorm) - xNorm) * Math.sqrt(2.0) / 2.0
        if (tmpDist > distMax) {
          distMax = tmpDist
          iMax = i
          xMax = xI
          yMax = yI
        }
      }
      i += 1
    }
    Array[Double](iMax, distMax, xMax, yMax)
  }

  def getElbowPointScore(distances: Array[Double]) = {
    val A = 0
    val B = distances.length - 1
    if (B - A < 2) {
      Array[Double](0, 0, 0, 0)
    }
    else {
      val dim = B - A + 1
      // max variables
      var iMax = 0
      var distMax: Double = 0
      var xMax: Double = 0
      var yMax: Double = 0
      // temporary variables
      var xNorm: Double = -1
      var yNorm: Double = -1
      var tmpDist: Double = 0
      val yB = distances(B)
      val yA = distances(A)
      var yI = 0.0d
      val xB = B
      val xA = A
      var xI = 0.0d
      var i = 0
      val it = distances.iterator
      while ( {
        it.hasNext
      }) {
        yI = it.next
        if (i == A) {
          distMax = 0
          iMax = i
          xMax = i
          yMax = yI
        }
        else if (i > A && i <= B) {
          xI = i
          xNorm = (xI - xA) / (xB - xA)
          yNorm = (yI - yB) / (yA - yB)
          tmpDist = ((1.0 - yNorm) - xNorm) * Math.sqrt(2.0) / 2.0
          if (tmpDist > distMax) {
            distMax = tmpDist
            iMax = i
            xMax = xI
            yMax = yI
          }
        }
        i += 1
      }
      Array[Double](iMax, distMax, xMax, yMax)
    }
  }

}

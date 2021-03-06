package factory

import org.apache.log4j.{Logger}
import org.apache.spark.sql.SparkSession

object Factory {

  def createSparkSession(appName: String, masterAddress: String, numberOfPartition:Int = 8, driverMemory:String = "1g", executorMemory:String = "8g"): SparkSession = {
    val spark = SparkSession
      .builder
      .appName(appName)
      .master(masterAddress)
      .config("spark.driver.bindAddress","localhost")
      .config("spark.scheduler.mode", "FIFO")
      .config("spark.driver.memory", driverMemory)
      .config("spark.executor.memory", executorMemory)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1024m")
      .config("spark.sql.caseSensitive", "true")

      .getOrCreate()
    spark.sqlContext.sql("set spark.sql.shuffle.partitions=%d".format(numberOfPartition))
    spark
  }

  def readJsonAsDataframe(pathToDataset: String, spark: SparkSession, appName:String): org.apache.spark.sql.DataFrame = {
    try {
      return spark.read.json(pathToDataset)
    } catch {
      case e: Exception => Logger.getLogger(appName).error(e);
    }
    null
  }

  def reatParquetAsJson(pathToDataset: String, spark: SparkSession, appName:String): org.apache.spark.sql.DataFrame = {
    try {
      return spark.read.parquet(pathToDataset)
    } catch {
      case e: Exception => Logger.getLogger(appName).error(e);
    }
    null
  }

  def read(pathToDataset: String, spark: SparkSession, appName:String) = {
    val df = reatParquetAsJson(pathToDataset,spark,appName)
    if (df == null){
      readJsonAsDataframe(pathToDataset,spark,appName)
    }
    df
  }

}

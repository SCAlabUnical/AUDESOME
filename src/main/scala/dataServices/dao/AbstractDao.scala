package dataServices.dao

import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class AbstractDao protected(_sqlSession: SparkSession) {


  protected val sqlSession = _sqlSession
  protected val RES_PATH = "src/main/resources/data/"

  protected def readData(path: String = null): Dataset[Row]

}

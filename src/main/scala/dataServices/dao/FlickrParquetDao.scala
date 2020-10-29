package dataServices.dao

import java.nio.file.{Files, Paths}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class FlickrParquetDao private(_sqlSession: SparkSession) extends AbstractDao(_sqlSession) {

  private val FLICKR_DEFAULT_PATH = "/datasets/rome/rome.parquet"
  private val logger = Logger.getLogger("FlickrParquetDao")

  /**
   * Return a spark dataframe adapted to workflow input data. This is the implementation
   * for Flickr Json data.
   *
   * @param path : Path to dataset or null if you want to use the default one
   * @return org.apache.spark.sql.Dataframe
   */
  override def readData(path: String = null): Dataset[Row] = {
    var dataPath: String = null
    if (path == null) {
      logger.log(Level.WARN, s"Path to dataset not provided, using the default : $FLICKR_DEFAULT_PATH")
      dataPath = s"$RES_PATH$FLICKR_DEFAULT_PATH"
    }
    else {
      dataPath = path
    }
    _sqlSession.read.parquet(dataPath)
  }


}

object FlickrParquetDao {

  def apply(_sqlSession: SparkSession): FlickrParquetDao = {
    Some(new FlickrParquetDao(_sqlSession)).get
  }

}
package dataServices.dao

import java.nio.file.{Files, Paths}

import factory.Factory
import dataServices.transform.DataTransform
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class FlickrJsonDao private(_sqlSession: SparkSession) extends AbstractDao(_sqlSession) {

  private val FLICKR_DEFAULT_PATH = "flickr/dataset.json"
  private val logger = Logger.getLogger("FlickrJsonDao")

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
    if (!Files.exists(Paths.get(dataPath)))
      throw new IllegalArgumentException(s"Files $dataPath not exists, check path to files")
    DataTransform.filterFlickrDataframe(Factory.readJsonAsDataframe(dataPath, _sqlSession, "FlickrJsonDao"))
  }

}

object FlickrJsonDao {

  def apply(_sqlSession: SparkSession): FlickrJsonDao = {
    Some(new FlickrJsonDao(_sqlSession)).get
  }
  
}
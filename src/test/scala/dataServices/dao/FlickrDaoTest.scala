package dataServices.dao

import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Before, Test}
import factory.Factory

class FlickrDaoTest {

  var sqlSession: SparkSession = _
  val sparkMasterAddress = "local[4]"

  @Before
  def setUp: Unit = {
    sqlSession = Factory.createSparkSession("FlickrDaoTest", sparkMasterAddress)
  }

  @Test
  def isJsonReading(): Unit = {
    //val df = FlickrJsonDao(sqlSession).readData(path = "src/main/resources/datasets/FlickrRome2017-25.json")
    val df = FlickrParquetDao(sqlSession).readData()
    df.show(50)
    Assert.fail()
  }

}

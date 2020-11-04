package dataServices.utils
import factory.Factory
import org.rogach.scallop.ScallopConf

class ConverterConfiguration(arguments: Seq[String]) extends ScallopConf(arguments) {
  val sparkHostname = opt[String](default = Option("local[4]"))
  val sparkApplication = opt[String](default = Option("AUDESOME"))
  val datasetPath = opt[String](required = true)
  val outputPath = opt[String](required = true)
  verify()
}

object ParquetConverter {
  def main(args: Array[String]): Unit = {
    val conf = new ConverterConfiguration(args)
    val spark = Factory.createSparkSession(conf.sparkApplication(), conf.sparkHostname())
    if (conf.datasetPath.isDefined){
      val jsonDataframe = spark.read.json(conf.datasetPath())
      var outputPath = "./converted.parquet"
      if (conf.outputPath.isDefined)
        outputPath = conf.outputPath()
      jsonDataframe.write.parquet(outputPath)
    }

  }
}

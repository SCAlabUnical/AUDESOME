package dataServices.transform

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructField, StructType}

import scala.collection.mutable

class MergeListsUDAF extends UserDefinedAggregateFunction {

  //May be substituited by nltk common words
  private val stopTerms = "?,1,2,3,4,5,6,7,8,9,0,:,#,foto,photo,foursquare".split(",")
  private val flickrTerms = "light,geotagged,squareformat,hdr,foto,photo,olympus,oculus,instagramapp,iphoneography,monochrome,biancoenero,bw,eos,powershot,macro,photographer,raw,brashears,iphone,instagram,olympus,flickr,canon,nikon,nikkor,sony,photo,foto,bb,nightshot".split(",")
  private val commonTerms = "europe,europa,italy,italia,italien,italie,roma,rom,rome,lazio,romeitaly,archivio,arti,contemporanee,river,english,british,centro,popolo,animali,romanum,olio,trajan,musica,centrale,romani,romana,roman,tela,leggera,spagna,mista,rooma,animals,kri,cb,italians,ruins,politica,uk,european,alessandro,ritratti,danza,cosplay,italian,latinium,architektur,monumenti".split(",")
  private val stopWords = scala.collection.mutable.Set[String]()

  stopWords ++= stopTerms
  stopWords ++= flickrTerms
  stopWords ++= commonTerms

  override def inputSchema: StructType = StructType(Seq(StructField("a", ArrayType(StringType))))

  override def bufferSchema: StructType = inputSchema

  override def dataType: DataType = ArrayType(StringType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, mutable.Seq[String]())

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val existing = buffer.getAs[mutable.Seq[String]](0)
    val newList = input.getAs[mutable.Seq[String]](0)
    var newListFiltered = newList diff stopWords.toSeq
    for (s <- newListFiltered){
      for (term <- stopTerms){
        if (s.contains(term)){
          newListFiltered = newListFiltered.filter(_ != s)
        }
      }
    }
    val result = (existing ++ newListFiltered)
    buffer.update(0, result)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = update(buffer1, buffer2)

  override def evaluate(buffer: Row): Any = buffer.getAs[mutable.Seq[String]](0)
}
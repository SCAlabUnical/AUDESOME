package dataServices.transform

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructField, StructType}

import scala.collection.mutable

class DistinctMergeListsUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Seq(StructField("a", ArrayType(StringType))))

  override def bufferSchema: StructType = inputSchema

  override def dataType: DataType = ArrayType(StringType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, mutable.Seq[String]())

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val existing = buffer.getAs[mutable.Seq[String]](0)
    val newList = input.getAs[mutable.Seq[String]](0)
    val result = (existing ++ newList).distinct
    buffer.update(0, result)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = update(buffer1, buffer2)

  override def evaluate(buffer: Row): Any = buffer.getAs[mutable.Seq[String]](0)
}

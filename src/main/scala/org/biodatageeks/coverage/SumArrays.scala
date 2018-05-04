package org.biodatageeks.coverage

import org.apache.log4j.LogManager
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class SumArrays extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: StructType =
    StructType(StructField("inputArray", ArrayType(IntegerType)) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType =
    StructType(StructField("bufferArray", ArrayType(IntegerType)) :: Nil)

  // This is the output type of your aggregatation function.
  override def dataType: DataType = ArrayType(IntegerType)

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new Array[Int](0);
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val a:Seq[Int] = buffer.getSeq[Int](0)//.getAs[Array[Int]](0)
    val b:Seq[Int] = input.getSeq[Int](0)//.getAs[Array[Int]](0)

    //val a:Array[Int] = buffer.getAs[Array[Int]](0)
    //val b:Array[Int] = input.getAs[Array[Int]](0)

    if (a == null || a.length == 0) {
      buffer(0)= b;
    } else {
      val out = new Array[Int](b.length)
      for(i<- 0 until a.length){
        out(i) = a(i) + b(i)
      }
      buffer(0)= out;
    }

  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val a:Seq[Int] = buffer1.getSeq[Int](0)//.getAs[Array[Int]](0)
    val b:Seq[Int] = buffer2.getSeq[Int](0)//.getAs[Array[Int]](0)

    if (a == null || a.length == 0) {
      buffer1(0)= b;
    } else {
      val out = new Array[Int](b.length)
      for(i<- 0 until a.length){
        out(i) = a(i) + b(i)
      }
      buffer1(0)= out;
    }
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getSeq[Int](0)
  }
}

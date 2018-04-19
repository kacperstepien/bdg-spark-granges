package org.biodatageeks.coverage

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import htsjdk.samtools.{Cigar, CigarOperator, TextCigarCodec}

import scala.collection.mutable.HashMap
class Coverage()  extends UserDefinedAggregateFunction {

  // Input Data Type Schema
  def inputSchema: StructType = StructType(
    Array(
    StructField("pos", IntegerType),
    StructField("chr", StringType),
    StructField("cigar", StringType)
  ))

  // Intermediate Schema
  def bufferSchema = StructType(Array(
    StructField("map",MapType(StructType(
      Array(
        StructField("chr", StringType),
        StructField("pos", IntegerType)
      )),IntegerType))))


  // Returned Data Type .
  def dataType: StructType = StructType(
    Array(
      StructField("pos", IntegerType),
      StructField("cov", IntegerType)
    ))

  // Self-explaining
  def deterministic = true

  // This function is called whenever key changes
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = new HashMap[(String, Int), Int]()
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    var position = input.getInt(0)
    val chr = input.getString(1)
    var covMap = buffer.getMap[(String,Int),Int](0)
    val cigar = TextCigarCodec.decode(input.getString(2))
    val cigInterator = cigar.getCigarElements.iterator()
    while (cigInterator.hasNext) {
      println(position + chr + cigar)
      val cigarElement = cigInterator.next()
      val cigarOpLength = cigarElement.getLength
      val cigarOp = cigarElement.getOperator
      cigarOp match {
        case CigarOperator.M | CigarOperator.X | CigarOperator.EQ =>
          var currPosition = 0
          //covArray(currPosition) = (((cr.sampleId, cr.condition, cr.chr, position), 1))
          while (currPosition < cigarOpLength) {
            if (covMap.keySet.contains((chr, position)))
              covMap += (chr, position) -> (covMap(chr, position) + 1)
            else
              covMap += (chr, position) -> (1)

            position += 1
            currPosition += 1
          }
        case CigarOperator.N | CigarOperator.D => position += cigarOpLength
        case _ => None
      }
    }
    buffer(0) = covMap
  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = buffer1.getMap[(String,Int),Int](0) ++ buffer2.getMap[(String,Int),Int](0).map{ case (k,v) => k -> (v + buffer1.getMap[(String,Int),Int](0).getOrElse(k,0)) }
    /*
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)*/
  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row) = {
    buffer.getMap[(String,Int),Int](0).toArray
  }
}

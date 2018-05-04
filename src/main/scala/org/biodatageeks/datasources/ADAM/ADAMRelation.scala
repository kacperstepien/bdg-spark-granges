package org.biodatageeks.datasources.ADAM

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.biodatageeks.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim

case class ADAMRecord (readInFragment:Int,
                            contigName:String,
                            start:Long,
                            oldPosition:Option[Long],
                            end:Long,
                            mapq:Long,
                            readName:String,
                            sequence:String,
                            qual:String,
                            cigar:String,
                            oldCigar:String,
                            basesTrimmedFromStart:Int,
                            basesTrimmedFromEnd:Int,
                            readPaired:Boolean,
                            properPair:Boolean,
                            readMapped:Boolean,
                            mateMapped:Boolean,
                            failedVendorQualityChecks:Boolean,
                            duplicateRead:Boolean,
                            readNegativeStrand:Boolean,
                            mateNegativeStrand:Boolean,
                            primaryAlignment:Boolean,
                            secondaryAlignment:Boolean,
                            supplementaryAlignment:Boolean,
                            mismatchingPositions:String,
                            origQual:String,
                            attributes:String,
                            recordGroupName:String,
                            recordGroupSample:String,
                            mateAlignmentStart:Option[Long],
                            mateContigName:String,
                            inferredInsertSize:Option[Long]
                           )

class ADAMRelation (path:String)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  val spark = sqlContext
    .sparkSession
  spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil

  private def parguet = spark.read.parquet(path)

  override def schema: org.apache.spark.sql.types.StructType = parguet.schema

  override def buildScan(requiredColumns: Array[String],  filters: Array[Filter]): RDD[Row] = parguet.rdd

}

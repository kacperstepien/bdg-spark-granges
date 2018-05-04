package org.biodatageeks.coverage

import java.util.concurrent.TimeUnit.NANOSECONDS

import breeze.linalg.max
import htsjdk.samtools.{CigarOperator, TextCigarCodec}
import org.apache.spark.sql.{Dataset, Encoders, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.biodatageeks.coverage.CoverageHistType.CoverageHistType
import org.biodatageeks.datasources.BAM.BAMRecord

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.control.Breaks._
import org.biodatageeks.coverage.CoverageTimers._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.{min, max}
import scala.collection.immutable.Map
/**
 * Created by kstepien on 15.04.18.
 */

case class CoverageReadRecord(sampleId:String,
                              condition:String,
                              chr:String,
                              position:Int,
                              cigar:htsjdk.samtools.Cigar,
                              mapq:Int,
                              samFlags: Int)

case class CoverageRecord(sampleId:String,
                          condition:String,
                          chr:String,
                          position:Int,
                          coverage:Double)


case class CoverageRecordSlim(chr:String,
                              position:Int,
                              coverage:Int)

case class CoverageRecordSlimHist(chr:String,
                                  position:Int,
                                  coverage:Array[Int],
                                  coverageTotal:Int)


case class CoverageIntervalRecord(sampleId:String,
                                  condition:String,
                                  chr:String,
                                  position_start:Int,
                                  postition_stop:Int,
                                  coverage:Double)

case class StatDist(statDist: Array[Double])

case class PartitionCoverage(covMap: HashMap[(String, Int), Array[Int]],
                             maxCigarLength: Int,
                             outputSize: Int,
                             chrMinMax: Array[(String,Int)]
                            )


case class PartitionCoverageHist(covMap: Map[(String, Int), (Array[Array[Int]],Array[Int])],
                                 maxCigarLength: Int,
                                 outputSize: Int,
                                 chrMinMax: Array[(String,Int)]
                                )

case class CoverageHistParam(
                              histType : CoverageHistType,
                              buckets: Array[Double]
                            )

class CoverageReadBAMFunctions(covReadDataset:Dataset[BAMRecord]) extends Serializable{


  def baseCoverageBaselineDataset(minMapq: Option[Int], numTasks: Option[Int] = None): Dataset[CoverageRecordSlim] ={
    val spark = covReadDataset.sparkSession

    import spark.implicits._
    BaseCoverageTimer.time {
      lazy val cov =numTasks match {
        case Some(n) => covReadDataset.repartition(n)
        case _ => covReadDataset
      }

      lazy val covQual = minMapq match {
        case Some(qual) => cov
        case _ => cov
      }
      CovTableWriterTimer.time {
        covQual
          //.instrument()
          .sort(asc("sampleId"),asc("contigName"),asc("start"))
          .mapPartitions{partIterator =>

            val covMap = new HashMap[(String, Int), Int]()
            for(cr <- partIterator) {
              val cigar = TextCigarCodec.decode(cr.cigar)
              val cigInterator = cigar.getCigarElements.iterator()
              var position = cr.start
              while (cigInterator.hasNext) {
                val cigarElement = cigInterator.next()
                val cigarOpLength = cigarElement.getLength
                val cigarOp = cigarElement.getOperator
                cigarOp match {
                  case CigarOperator.M | CigarOperator.X | CigarOperator.EQ =>
                    var currPosition = 0
                    //covArray(currPosition) = (((cr.sampleId, cr.condition, cr.chr, position), 1))
                    while (currPosition < cigarOpLength) {
                      if(covMap.keySet.contains((cr.contigName, position)) )
                        covMap+= (cr.contigName, position) -> (covMap(cr.contigName, position) + 1 )
                      else
                        covMap += (cr.contigName, position) -> (1)

                      position += 1
                      currPosition += 1
                    }
                  case CigarOperator.N | CigarOperator.D => position += cigarOpLength
                  case _ => None
                }
              }
            }
            Iterator(covMap)
          }(Encoders.kryo[mutable.HashMap[(String,Int),Int]])
          .flatMap(r => r)

          .groupBy("_1")
          .sum("_2")
          .map(r=>CoverageRecordSlim(r.getStruct(0).getString(0), r.getStruct(0).getInt(1), r.getLong(1).toInt))
          //.mapGroups{case ((chr, pos), cov) => CoverageRecordSlim(chr, pos, cov.reduce(_._2+_._2)) }
          //.reduceGroups((a, b) => (a._1, a._2 + b._2))
          //.map(_._2)
          //.map {case ((chr, pos), cov) => CoverageRecordSlim(chr, pos, cov) }
      }
    }
  }


  def baseCoverageHistDataset(minMapq: Option[Int], numTasks: Option[Int] = None, coverageHistParam: CoverageHistParam) : Dataset[CoverageRecordSlimHist] = {
    val spark = covReadDataset.sparkSession
    var start = System.nanoTime()
    import spark.implicits._
    lazy val cov =numTasks match {
      case Some(n) => covReadDataset.repartition(n)
      case _ => covReadDataset
    }

    lazy val covQual = minMapq match {
      case Some(qual) => cov //FIXME add filtering
      case _ => cov
    }
    start = System.nanoTime()
    lazy val partCov = covQual
      .sort(asc("contigName"),asc("start"))
      .mapPartitions { partIterator =>
        val covMap = new HashMap[(String, Int), (Array[Array[Int]], Array[Int])]()
        val numSubArrays = 10000
        val subArraySize = 250000000 / numSubArrays
        val chrMinMax = new ArrayBuffer[(String, Int)]()
        var maxCigarLength = 0
        var lastChr = "NA"
        var lastPosition = 0
        var outputSize = 0
        for (cr <- partIterator) {
          val cigar = TextCigarCodec.decode(cr.cigar)
          val cigInterator = cigar.getCigarElements.iterator()
          var position = cr.start
          val cigarStartPosition = position
          while (cigInterator.hasNext) {
            val cigarElement = cigInterator.next()
            val cigarOpLength = cigarElement.getLength
            val cigarOp = cigarElement.getOperator
            cigarOp match {
              case CigarOperator.M | CigarOperator.X | CigarOperator.EQ =>
                var currPosition = 0
                while (currPosition < cigarOpLength) {
                  val subIndex = position % numSubArrays
                  val index = position / numSubArrays

                  if (!covMap.keySet.contains(cr.contigName, index)) {
                    covMap += (cr.contigName, index) -> (Array.ofDim[Int](numSubArrays,coverageHistParam.buckets.length),Array.fill[Int](subArraySize)(0) )
                  }
                  val params = coverageHistParam.buckets.sortBy(r=>r)
                  if(coverageHistParam.histType == CoverageHistType.MAPQ) {
                    breakable {
                      for (i <- 0 until params.length) {
                        if ( i < params.length-1  && cr.mapq >= params(i) && cr.mapq < params(i+1)) {
                          covMap(cr.contigName, index)._1(subIndex)(i) += 1
                          break
                        }
                      }

                    }
                    if (cr.mapq >= params.last) covMap(cr.contigName, index)._1(subIndex)(params.length-1) += 1
                  }
                  else throw new Exception("Unsupported histogram parameter")


                  covMap(cr.contigName, index)._2(subIndex) += 1

                  position += 1
                  currPosition += 1
                  /*add first*/
                  if (outputSize == 0) chrMinMax.append((cr.contigName, position))
                  if (covMap(cr.contigName, index)._2(subIndex) == 1) outputSize += 1

                }
              case CigarOperator.N | CigarOperator.D => position += cigarOpLength
              case _ => None
            }
          }
          val currLength = position - cigarStartPosition
          if (maxCigarLength < currLength) maxCigarLength = currLength
          lastPosition = position
          lastChr = cr.contigName
        }
        chrMinMax.append((lastChr, lastPosition))
        Iterator(PartitionCoverageHist(covMap.toMap, maxCigarLength, outputSize, chrMinMax.toArray))//.iterator
      }.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("partCov="+NANOSECONDS.toSeconds(System.nanoTime() - start));
    start = System.nanoTime()
    //val maxCigarLengthGlobal = partCov.map(r => r.maxCigarLength)
    //  .reduce((a, b) => breeze.linalg.max(a, b))
    //println("maxCigarLengthGlobal="+maxCigarLengthGlobal);
    val maxCigarLengthGlobal = partCov.agg(org.apache.spark.sql.functions.max($"maxCigarLength")).head().getInt(0)
    //println("maxCigarLengthGlobal="+partCov.agg(org.apache.spark.sql.functions.max($"maxCigarLength")).head().getInt(0));
    println("maxCigarLengthGlobal="+NANOSECONDS.toSeconds(System.nanoTime() - start));
    start = System.nanoTime()
    lazy val combOutput = partCov.mapPartitions { partIterator =>
      /*split for reduction basing on position and max cigar length across all partitions - for gap alignments*/
      val partitionCoverageArray = (partIterator.toArray)
      val partitionCoverage = partitionCoverageArray(0)
      val chrMinMax = partitionCoverage.chrMinMax
      lazy val output = new Array[Array[CoverageRecordSlimHist]](2)
      lazy val outputArray = new Array[CoverageRecordSlimHist](partitionCoverage.outputSize)
      lazy val outputArraytoReduce = new ArrayBuffer[CoverageRecordSlimHist]()
      val covMap = partitionCoverage.covMap
      var cnt = 0
      for (key <- covMap.keys) {
        var locCnt = 0
        val covs = covMap.get(key).get
        for (i<-0 until covs._1.length) {
          if (covs._2(i) > 0) {
            val position = key._2 * 10000 + locCnt
            if(key._1 == chrMinMax.head._1 && position <= chrMinMax.head._2 + maxCigarLengthGlobal ||
              key._1 == chrMinMax.last._1 && position >= chrMinMax.last._2 - maxCigarLengthGlobal )
              outputArraytoReduce.append(CoverageRecordSlimHist(key._1,position,covs._1(i),covs._2(i)))
            else
              outputArray(cnt) = CoverageRecordSlimHist(key._1,position ,covs._1(i),covs._2(i))
            cnt += 1
          }
          locCnt += 1
        }
      } /*only records from the beginning and end of the partition for reduction the rest pass-through */
      output(0) = outputArray.filter(r=> r!=null )
      output(1) = outputArraytoReduce.toArray
      Iterator(output)
    }(Encoders.kryo[Array[Array[CoverageRecordSlimHist]]])
    println("combOutput="+NANOSECONDS.toSeconds(System.nanoTime() - start));
    start = System.nanoTime()
    //partCov.unpersist()
    val sa:SumArrays = new SumArrays();
    lazy val covReduced =  combOutput
      .flatMap(r=>r.array(1))
      .map(r=>((r.chr,r.position),r))
      .groupBy("_1").agg(sa($"_2.coverage"),sum("_2.coverageTotal"))

        //.map(r=>CoverageRecordSlimHist(r.getStruct(0).getString(0), r.getStruct(0).getInt(1), r.getAs[Array[Int]](1),r.getLong(2).toInt))
      .map(r=>CoverageRecordSlimHist(r.getStruct(0).getString(0), r.getStruct(0).getInt(1), r.getSeq[Int](1).toArray,r.getLong(2).toInt))
      //.groupByKey(_._1)
      //.reduceGroups((a,b)=>(a._1,CoverageRecordSlimHist(a._2.chr,a._2.position,sumArrays(a._2.coverage,b._2.coverage),a._2.coverageTotal+b._2.coverageTotal)))
      //.map(_._2._2)

      //.groupBy("_1")
      //.sum("_2")
      //.map(r=>CoverageRecordSlim(r.getStruct(0).getString(0), r.getStruct(0).getInt(1), r.getLong(1).toInt))
    println("covReduced="+NANOSECONDS.toSeconds(System.nanoTime() - start));
    start = System.nanoTime()
    partCov.unpersist()
    val output = combOutput.flatMap(r => (r.array(0)))
      .union(covReduced)
    println("union="+NANOSECONDS.toSeconds(System.nanoTime() - start));

    output
  }

  def baseCoverageDataset(minMapq: Option[Int], numTasks: Option[Int] = None, sorted: Boolean):Dataset[CoverageRecordSlim] ={
    val spark = covReadDataset.sparkSession

    import spark.implicits._
    BaseCoverageTimer.time {

      lazy val cov =numTasks match {
        case Some(n) => covReadDataset.repartition(n)
        case _ => covReadDataset
      }

      lazy val covQual = minMapq match {
        case Some(qual) => cov //FIXME add filtering
        case _ => cov
      }
      CovTableWriterTimer.time {
        lazy val partCov ={ sorted match {
          case true => covQual//.instrument()
          case _ => covQual.sort(asc("contigName"),asc("start"))
        }}.mapPartitions { partIterator =>
            val covMap = new HashMap[(String, Int), Array[Int]]()
            val numSubArrays = 10000
            val subArraySize = 250000000 / numSubArrays
            val chrMinMax = new ArrayBuffer[(String, Int)]()
            var maxCigarLength = 0
            var lastChr = "NA"
            var lastPosition = 0
            var outputSize = 0
            for (cr <- partIterator) {
              val cigar = TextCigarCodec.decode(cr.cigar)
              val cigInterator = cigar.getCigarElements.iterator()
              var position = cr.start
              val cigarStartPosition = position
              while (cigInterator.hasNext) {
                val cigarElement = cigInterator.next()
                val cigarOpLength = cigarElement.getLength
                val cigarOp = cigarElement.getOperator
                if(cigarOp == CigarOperator.M || cigarOp == CigarOperator.X || cigarOp == CigarOperator.EQ) {
                  var currPosition = 0
                  while (currPosition < cigarOpLength) {
                    val subIndex = position % numSubArrays
                    val index = position / numSubArrays

                    if (!covMap.keySet.contains(cr.contigName, index)) {
                      covMap += (cr.contigName, index) -> Array.fill[Int](subArraySize)(0)
                    }
                    covMap(cr.contigName, index)(subIndex) += 1
                    position += 1
                    currPosition += 1

                    /*add first*/
                    if (outputSize == 0) chrMinMax.append((cr.contigName, position))
                    if (covMap(cr.contigName, index)(subIndex) == 1) outputSize += 1

                  }
                }
                else if (cigarOp == CigarOperator.N || cigarOp == CigarOperator.D)  position += cigarOpLength
              }
              val currLength = position - cigarStartPosition
              if (maxCigarLength < currLength) maxCigarLength = currLength
              lastPosition = position
              lastChr = cr.contigName
            }
            chrMinMax.append((lastChr, lastPosition))
            Array(PartitionCoverage(covMap, maxCigarLength, outputSize, chrMinMax.toArray)).iterator
          }.persist(StorageLevel.MEMORY_AND_DISK_SER)
        val maxCigarLengthGlobal = partCov.map(r => r.maxCigarLength).reduce((a, b) => breeze.linalg.max(a, b))
        lazy val combOutput = partCov.mapPartitions { partIterator =>
          /*split for reduction basing on position and max cigar length across all partitions - for gap alignments*/
          val partitionCoverageArray = (partIterator.toArray)
          val partitionCoverage = partitionCoverageArray(0)
          val chrMinMax = partitionCoverage.chrMinMax
          lazy val output = new Array[Array[CoverageRecordSlim]](2)
          lazy val outputArray = new Array[CoverageRecordSlim](partitionCoverage.outputSize)
          lazy val outputArraytoReduce = new ArrayBuffer[CoverageRecordSlim]()
          val covMap = partitionCoverage.covMap
          var cnt = 0
          for (key <- covMap.keys) {
            var locCnt = 0
            for (value <- covMap.get(key).get) {
              if (value > 0) {
                val position = key._2 * 10000 + locCnt
                if(key._1 == chrMinMax.head._1 && position <= chrMinMax.head._2 + maxCigarLengthGlobal ||
                    key._1 == chrMinMax.last._1 && position >= chrMinMax.last._2 - maxCigarLengthGlobal )
                  outputArraytoReduce.append(CoverageRecordSlim(key._1,position , value))
                else
                  outputArray(cnt) = (CoverageRecordSlim(key._1,position , value))
                cnt += 1
              }
              locCnt += 1
            }
          } /*only records from the beginning and end of the partition for reduction the rest pass-through */
          output(0) = outputArray.filter(r=> r!=null )
          output(1) = outputArraytoReduce.toArray
          Iterator(output)
        }(Encoders.kryo[Array[Array[CoverageRecordSlim]]])
        //partCov.unpersist()
        lazy val covReduced =  combOutput.
          flatMap(r=>r.array(1))
          .map(r=>((r.chr,r.position),r))
              .groupByKey(r=>r._1)
            .reduceGroups((a,b) => (a._1,CoverageRecordSlim(a._1._1,a._1._2,a._2.coverage+b._2.coverage)))
          .map(_._2._2)
        partCov.unpersist()
        combOutput.flatMap(r => (r.array(0)))
          .union(covReduced)
      }
    }
  }

  def baseCoverageMultiSampleDataset(minMapq: Option[Int], numTasks: Option[Int] = None): Dataset[CoverageRecord] ={
    val spark = covReadDataset.sparkSession

    import spark.implicits._
    BaseCoverageTimer.time {
      lazy val cov =numTasks match {
        case Some(n) => covReadDataset.repartition(n)
        case _ => covReadDataset
      }

      lazy val covQual = minMapq match {
        case Some(qual) => cov
        case _ => cov
      }
      CovTableWriterTimer.time {
        val covMap = new mutable.HashMap[(String,String,String,Int),Int]()
        covQual
          //.instrument()
          .sort(asc("sampleId"),asc("contigName"),asc("start"))
          .mapPartitions{partIterator =>
            val covMap = new mutable.HashMap[(String, String, String, Int), Int]()
            for(cr <- partIterator) {
              val cigar = TextCigarCodec.decode(cr.cigar)
              val cigInterator = cigar.getCigarElements.iterator()
              var position = cr.start
              while (cigInterator.hasNext) {
                val cigarElement = cigInterator.next()
                val cigarOpLength = cigarElement.getLength
                val cigarOp = cigarElement.getOperator
                cigarOp match {
                  case CigarOperator.M | CigarOperator.X | CigarOperator.EQ =>
                    var currPosition = 0
                    while (currPosition < cigarOpLength) {
                      //covArray(currPosition) = (((cr.sampleId, cr.condition, cr.chr, position), 1))
                      if(covMap.keySet.contains((cr.sampleId, cr.baseq, cr.contigName, position)) )
                        covMap+= (cr.sampleId, cr.baseq, cr.contigName, position) -> (covMap(cr.sampleId, cr.baseq, cr.contigName, position) + 1 )
                      else
                        covMap += (cr.sampleId, cr.baseq, cr.contigName, position) -> (1)

                      position += 1
                      currPosition += 1
                    }
                  case CigarOperator.N | CigarOperator.D => position += cigarOpLength
                  case _ => None
                }
              }
            }
            Iterator(covMap)
          }(Encoders.kryo[mutable.HashMap[(String,String,String,Int),Int]]).flatMap(r => r)
          .groupByKey(r=> r._1)
            .reduceGroups((a,b)=>(a._1,a._2+b._2))
          .map(r=>r._2)
          .map { case ((sampleId, condition, chr, pos), cov) => CoverageRecord(sampleId, condition, chr, pos, cov) }
      }
    }
  }


  def computeReadCountsDataset = {
    val spark = covReadDataset.sparkSession

    import spark.implicits._
    val countsMap = new HashMap[String,Long]()

    covReadDataset
      .map(r=>(r.sampleId,1))
      .groupByKey(r=>r._1)
        .reduceGroups((a,b)=>(a._1,a._2+b._2))
      .map(r=>r._2)
      .collect()
      .foreach(r=> countsMap(r._1) = (r._2) )
    countsMap
  }
}

class CoverageFunctionsSlim(coverageDataset:Dataset[CoverageRecordSlim]) extends Serializable {

  val sqlContext = new SQLContext(coverageDataset.sparkSession.sparkContext)

  /**
    *
    * @param path
    */
  def saveCoverageAsParquet(path: String, sort:Boolean = false) = {
    val covDF = coverageDataset.toDF()
    if(sort) {
      covDF
        .orderBy("contigName", "start")
        .write
        .option("compression", "gzip")
        .save(path)
    }
    else{
      covDF
        .write
        .option("compression", "gzip")
        .save(path)
    }
  }
}

class CoverageFunctionsSlimHist(coverageDataset:Dataset[CoverageRecordSlimHist]) extends Serializable {

  val sqlContext = new SQLContext(coverageDataset.sparkSession.sparkContext)

  /**
    *
    * @param path
    */
  def saveCoverageAsParquet(path: String, sort:Boolean = false) = {
    val covDF = coverageDataset.toDF()
    if(sort) {
      covDF
        .orderBy("contigName", "start")
        .write
        .option("compression", "gzip")
        .save(path)
    }
    else{
      covDF
        .write
        .option("compression", "gzip")
        .save(path)
    }
  }
}


class CoverageFunctions(coverageDataset:Dataset[CoverageRecord]) extends Serializable {

  val sqlContext = new SQLContext(coverageDataset.sparkSession.sparkContext)

  /**
    *
    * @param path
    */
  def saveCoverageAsParquet(path: String) = {
    val covDF = coverageDataset.toDF()
    covDF
      .orderBy("contigName", "start")
      .write
      .option("compression", "gzip")
      .save(path)
  }
}

object CoverageReadBAMFunctions {

  implicit def addCoverageReadBAMFunctions(dataset: Dataset[BAMRecord]) = {
    new CoverageReadBAMFunctions(dataset)

  }
}

  object CoverageFunctions {
    implicit def addCoverageFunctions(dataset: Dataset[CoverageRecord]) = new
        CoverageFunctions(dataset)
  }

  object CoverageFunctionsSlim {
    implicit def addCoverageFunctionsSlim(dataset: Dataset[CoverageRecordSlim]) = new
        CoverageFunctionsSlim(dataset)
  }

  object CoverageFunctionsSlimHist {
    implicit def addCoverageFunctionsSlimHist(dataset: Dataset[CoverageRecordSlimHist]) = new
        CoverageFunctionsSlimHist(dataset)
  }
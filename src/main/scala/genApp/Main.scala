/**
  * Licensed to Big Data Genomics (BDG) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The BDG licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package genApp

import java.io.PrintWriter
import java.util.Date

import ncl.NCListsJoinStrategy
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.formats.avro.Feature
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.apache.spark.sql.types._

import scala.util.Random

object Main {
  case class RecordData1(start1: Long, end1: Long) extends Serializable
  case class RecordData2(start2: Long, end2: Long) extends Serializable
  def main(args: Array[String]) {
    if (!("random".equals(args(0))) && !("bio".equals(args(0)))) {
      System.err.println("first argument should be one of two: random; bio")
      System.exit(1)
    }

    if ("random".equals(args(0))) {
      if (args.length < 5) {
        System.err.println("four argument required, step start stop loops")
        System.exit(1)
      } else {
        randomTest(args)
      }
    }

    if ("bio".equals(args(0))) {
      if (args.length < 7) {
        System.err.println("four argument required, step start stop loops")
        System.exit(1)
      } else {
        bioTest(args)
      }
    }
  }

  def show_timing[T](proc: => T): T = {
    val start=System.nanoTime()
    val res = proc // call the code
    val end = System.nanoTime()
    println("Time elapsed: " + (end-start)/1000 + " microsecs")
    res
  }

  def log(text:String,pw:PrintWriter): Unit = {
    println(text)
    pw.write(text+"\n")
  }

  def bioTest(args: Array[String]): Unit = {
    // PrintWriter
    import java.io._
    import java.text.SimpleDateFormat
    val fileName = new SimpleDateFormat("'bioTest'yyyyMMddHHmm'.txt'").format(new Date())
    val pw = new PrintWriter(new File(fileName))


    val step = args(1).toInt
    val start = args(2).toInt
    val stop = args(3).toInt
    val loops = args(4).toInt
    val spark = SparkSession
      .builder()
      .appName("ExtraStrategiesGenApp")
      .config("spark.master", "local")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    Random.setSeed(4242)


    //var features: FeatureRDD = sc.loadFeatures(args(5))
    var alignments1: AlignmentRecordRDD = sc.loadAlignments(args(5))
    var alignments2: AlignmentRecordRDD = sc.loadAlignments(args(6))

    //var featuresRdd: RDD[Feature] = features.rdd
    var alignments1Rdd: RDD[AlignmentRecord] = alignments1.rdd
    var alignments2Rdd: RDD[AlignmentRecord] = alignments2.rdd
    //get only interesting columns

    //val fRdd = featuresRdd.map(rec => Row(rec.getStart(), rec.getEnd()));
    val a1Rdd = alignments1Rdd.map(rec => Row(rec.getStart(), rec.getEnd()));
    val a2Rdd = alignments2Rdd.map(rec => Row(rec.getStart(), rec.getEnd()));
    //val alignmentsSchema = StructType(Seq(StructField("start2", LongType), StructField("end2", LongType)))
    //val featuresSchema = StructType(Seq(StructField("start1", LongType), StructField("end1", LongType)))

    // create DataFrames from RDD's
    //val alignmentsDF = sqlContext.createDataFrame(aRdd, alignmentsSchema)
    //val featuresDF = sqlContext.createDataFrame(fRdd, featuresSchema)

    //alignmentsDF.createOrReplaceTempView("alignments")
    //featuresDF.createOrReplaceTempView("features")





    for (i <-start to stop) {
      var size = i*step
      /*var rdd1 = sc.parallelize((1 to size).map(x => {val r1=Random.nextInt(1000).toLong; val r2=Random.nextInt(1000).toLong; if (r1<=r2) {Row(r1,r2)} else {Row(r2,r1)}}))
      var rdd2 = sc.parallelize((1 to size).map(x => {val r1=Random.nextInt(1000).toLong; val r2=Random.nextInt(1000).toLong; if (r1<=r2) {Row(r1,r2)} else {Row(r2,r1)}}))*/
      //var rdd1 = sc.parallelize(fRdd.takeSample(false,size,4242))
      var rdd1 = sc.parallelize(a1Rdd.take(size))
      var rdd2 = sc.parallelize(a2Rdd.take(size))
      //var rdd1 = sc.parallelize(a1Rdd.takeSample(false,size,4242))
      //var rdd2 = sc.parallelize(a2Rdd.takeSample(false,size,4242))

      val schema1 = StructType(Seq(StructField("start1", LongType), StructField("end1", LongType)))
      val schema2 = StructType(Seq(StructField("start2", LongType), StructField("end2", LongType)))
      var ds1 = sqlContext.createDataFrame(rdd1, schema1)
      ds1.createOrReplaceTempView("s1")
      var ds2 = sqlContext.createDataFrame(rdd2, schema2)
      ds2.createOrReplaceTempView("s2")

      val sqlQuery = "select count(*) from s1 JOIN s2 on (end1>=start2 and start1<=end2)"

      log(size+" Size"+ "\t" +"NCListsJoin"+ "\t" +"IntervalTreeJoin",pw)
      for (j <- 1 to loops) {
        spark.experimental.extraStrategies = Nil
        var start = System.nanoTime()
        sqlContext.sql(sqlQuery).count
        var end = System.nanoTime()

        if (i==1 && j==1) {
          //repeat test, because of lazy loading
          start = System.nanoTime()
          sqlContext.sql(sqlQuery).count
          end = System.nanoTime()
        }

        var cartesianTime = (end - start) / 1000
        spark.experimental.extraStrategies = new IntervalTreeJoinStrategy(spark) :: Nil
        start = System.nanoTime()
        sqlContext.sql(sqlQuery).count
        end = System.nanoTime()
        var intervalTime = (end - start) / 1000
        log(j + "/" + loops + "\t" + cartesianTime + "\t" + intervalTime, pw)
      }
    }

    pw.close
  }

  def randomTest(args: Array[String]): Unit = {
    // PrintWriter
    import java.io._
    import java.text.SimpleDateFormat
    val fileName = new SimpleDateFormat("'randomTest'yyyyMMddHHmm'.txt'").format(new Date())
    val pw = new PrintWriter(new File(fileName))


    val step = args(1).toInt
    val start = args(2).toInt
    val stop = args(3).toInt
    val loops = args(4).toInt
    val spark = SparkSession
      .builder()
      .appName("ExtraStrategiesGenApp")
      .config("spark.master", "local")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    Random.setSeed(4242)

    for (i <-start to stop) {
      var size = i*step
      var rdd1 = sc.parallelize((1 to size).map(x => {val r1=Random.nextInt(1000).toLong; val r2=Random.nextInt(1000).toLong; if (r1<=r2) {Row(r1,r2)} else {Row(r2,r1)}}))
      var rdd2 = sc.parallelize((1 to size).map(x => {val r1=Random.nextInt(1000).toLong; val r2=Random.nextInt(1000).toLong; if (r1<=r2) {Row(r1,r2)} else {Row(r2,r1)}}))
      val schema1 = StructType(Seq(StructField("start1", LongType), StructField("end1", LongType)))
      val schema2 = StructType(Seq(StructField("start2", LongType), StructField("end2", LongType)))
      var ds1 = sqlContext.createDataFrame(rdd1, schema1)
      ds1.createOrReplaceTempView("s1")
      var ds2 = sqlContext.createDataFrame(rdd2, schema2)
      ds2.createOrReplaceTempView("s2")

      val sqlQuery = "select count(*) from s1 JOIN s2 on (end1>=start2 and start1<=end2)"

      log(size+" Size"+ "\t" +"NCListsJoin"+ "\t" +"IntervalTreeJoin",pw)
      for (j <- 1 to loops) {
        spark.experimental.extraStrategies = new NCListsJoinStrategy(spark) :: Nil
        var start = System.nanoTime()
        sqlContext.sql(sqlQuery).count
        var end = System.nanoTime()

        if (i==1 && j==1) {
          //repeat test, because of lazy loading
          start = System.nanoTime()
          sqlContext.sql(sqlQuery).count
          end = System.nanoTime()
        }

        var cartesianTime = (end - start) / 1000
        spark.experimental.extraStrategies = new IntervalTreeJoinStrategy(spark) :: Nil
        start = System.nanoTime()
        sqlContext.sql(sqlQuery).count
        end = System.nanoTime()
        var intervalTime = (end - start) / 1000
        log(j + "/" + loops + "\t" + cartesianTime + "\t" + intervalTime, pw)
      }
    }

    pw.close
  }
}
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
package org.biodatageeks.coverage

import java.io.{OutputStreamWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.sql.{Dataset, SparkSession}
import org.bdgenomics.utils.instrumentation.{Metrics, MetricsListener, RecordedMetrics}
import org.biodatageeks.datasources.BAM.{BAMDataSource, BAMRecord}
import org.biodatageeks.coverage.CoverageReadBAMFunctions._
import org.biodatageeks.coverage.CoverageReadADAMFunctions._
import org.biodatageeks.coverage.CoverageFunctionsSlimHist._
import org.biodatageeks.datasources.ADAM.ADAMRecord

object Main {
  val metricsListener = new MetricsListener(new RecordedMetrics())
  val writer = new PrintWriter(new OutputStreamWriter(System.out))
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("ExtraStrategiesGenApp")
      .config("spark.master", "local")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
 Metrics.initialize(spark.sparkContext)
 sc.addSparkListener(metricsListener)
import spark.implicits._
    //val bamPath = "/home/kacper/Pobrane/n7_10M.bam"
 val bamPath = "/home/kacper/Pobrane/exome/FIRST_300.HG00096.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam"
 val dataset = spark
   .read
   .format("org.biodatageeks.datasources.BAM.BAMDataSource")
   .load(bamPath)
   .as[BAMRecord]

    /*val adamPath = "/home/kacper/Pobrane/exome/FIRST_300.HG00096.mapped.ILLUMINA.bwa.GBR.exome.20111114.adam"
    val datasetAdam = spark
      .read
      .format("org.biodatageeks.datasources.ADAM.ADAMDataSource")
      .load(adamPath)
      .as[ADAMRecord]*/

    var start = System.nanoTime()
 var coverage = dataset.baseCoverageHistDataset(None, None,CoverageHistParam(CoverageHistType.MAPQ,Array(10,20,30,40))).count
    //println((System.nanoTime() - start) / 1000)
    println(NANOSECONDS.toSeconds(System.nanoTime() - start));
    //var fileName = new SimpleDateFormat("'/home/kacper/coverage_BAM_'yyyyMMddHHmm'.parquet'").format(new Date())
 //coverage.saveCoverageAsParquet(fileName,sort=false)

/*
    var start = System.nanoTime()
    var coverage = datasetAdam.baseCoverageHistDataset(None, None,CoverageHistParam(CoverageHistType.MAPQ,Array(10,20,30,40))).count
    //println((System.nanoTime() - start) / 1000)
    println(NANOSECONDS.toSeconds(System.nanoTime() - start));*/
    //var fileName = new SimpleDateFormat("'/home/kacper/coverage_ADAM_'yyyyMMddHHmm'.parquet'").format(new Date())
    //coverage.saveCoverageAsParquet(fileName,sort=false)

 Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
 writer.flush();
}
}
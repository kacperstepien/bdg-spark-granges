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

import org.apache.spark.sql.{Dataset, SparkSession}
import org.biodatageeks.datasources.BAM.{BAMDataSource, BAMRecord}
import org.biodatageeks.coverage.CoverageReadDatasetFunctions._
object Main {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("ExtraStrategiesGenApp")
      .config("spark.master", "local")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    sqlContext.udf.register("coverage", new Coverage())
    spark.sql(s"DROP TABLE IF EXISTS bam")
    spark.sql(
      s"""
         |CREATE TABLE bam
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "/home/kacper/Pobrane/exome/FIRST_300.HG00096.mapped.ILLUMINA.bwa.GBR.exome.20111114.bam")
         |
      """.stripMargin)
    val ds:Dataset[BAMRecord]=spark.sql("select * from bam").transform[BAMRecord](r => new BAMRecord(r.))
    spark.read.
    spark.sql(
      "select coverage(start,contigName,cigar) from bam limit 100"
    ).show
  }
}
package org.biodatageeks.rangejoins.methods.IntervalTree

import org.apache.spark.sql.catalyst.InternalRow
import org.biodatageeks.rangejoins.IntervalTree.{IntervalTreeHTS, IntervalWithRow}

class IntervalTreeHTSChromosome(allRegions: List[(String,IntervalWithRow[Int])]) extends Serializable {

  val intervalTreeHashMap:Map[String,IntervalTreeHTS[InternalRow]] = allRegions.groupBy(_._1).map(x => {
    val it = new IntervalTreeHTS[InternalRow]()
    x._2.map(y => it.put(y._2.start,y._2.end,y._2.row))
    (x._1, it)
  })

  def getIntervalTreeByChromosome(chr:String): IntervalTreeHTS[InternalRow] = {

    return intervalTreeHashMap(chr)
  }
}

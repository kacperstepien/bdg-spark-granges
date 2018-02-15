package org.biodatageeks.rangejoins.NCList

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object NCListBuilder {
  def build[T](list: List[(Interval[Int], T)]): NCList = {
    val topNCList = NCList(Array.empty[NCList], 0, Array.empty[Int])
    var landingNCList = NCList(Array.empty[NCList], 0, Array.empty[Int])

    val listWithIndices = list.zipWithIndex.map{case (k,v) => (v,k)}.toArray
    val sortedIndices = listWithIndices.sortWith((x, y) => x._2._1.end > y._2._1.end)
      .sortWith((x, y) => x._2._1.start < y._2._1.start)
      .map(x => x._1)


    val stack = mutable.ArrayStack[NCListBuildingStack]()

    sortedIndices.foreach (
      rgid => {
      val currentEnd = listWithIndices(rgid)._2._1.end
      while(!stack.isEmpty && listWithIndices(stack.top.rgid)._2._1.end < currentEnd)
        stack.pop

      landingNCList = if (stack.isEmpty) topNCList else stack.top.ncList

      val stackElt = appendNCListElt(landingNCList, rgid)
      stack.push(stackElt)
    })

    topNCList
  }

   def appendNCListElt(landingNCList: NCList, rgid: Int): NCListBuildingStack = {
     landingNCList.childrenBuf +:= NCList(Array.empty[NCList], 0, Array.empty[Int])
     val childrenNCList = landingNCList.childrenBuf.last
     val stackElt = NCListBuildingStack(childrenNCList,rgid)
     landingNCList.rgidBuf +:= rgid
     landingNCList.nChildren = landingNCList.nChildren+1

     stackElt
   }
}

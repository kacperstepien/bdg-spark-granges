package ncl

import org.apache.spark.rdd.RDD

import scala.collection.mutable

object NCListBuilder {
  def build(rdd: RDD[org.apache.spark.sql.Row]): NCList = {

    var topNCList = NCList(Array.empty[NCList], 0, Array.empty[Int])
    var landingNCList = NCList(Array.empty[NCList], 0, Array.empty[Int])

    var rddWithIndices = rdd.map(x => (x(0).toString.toInt, x(1).toString.toInt)).zipWithIndex().map{case (k,v) => (v,k)}
    var sortedIndices = rddWithIndices.sortBy(_._2._2,false).sortBy(_._2._1).map(_._1).collect()


    var stack = mutable.Stack[NCListBuildingStack]()


    for( i <- 0 until sortedIndices.length) {
      var rgid = sortedIndices(i).toInt
      var currentEnd = rddWithIndices.lookup(rgid).map(_._2).head
      while(!stack.isEmpty && rddWithIndices.lookup(stack.top.rgid).map(_._2).head < currentEnd)
        stack.pop

      landingNCList = if (stack.isEmpty) topNCList else stack.top.ncList

      var stackElt = appendNCListElt(landingNCList, rgid)
      stack.push(stackElt)
    }

    topNCList
  }

   def appendNCListElt(landingNCList: NCList, rgid: Int): NCListBuildingStack = {
     var nChildren = landingNCList.nChildren
     landingNCList.childrenBuf :+= NCList(Array.empty[NCList], 0, Array.empty[Int])
     var childrenNCList = landingNCList.childrenBuf.last
     var stackElt = NCListBuildingStack(childrenNCList,rgid)
     landingNCList.rgidBuf :+= rgid
     landingNCList.nChildren = landingNCList.nChildren+1

     stackElt
   }
}

package ncl

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import util.control.Breaks._

object NCListProcessor {

  def findOverlaps(rddLeft: RDD[org.apache.spark.sql.Row], rddRight: RDD[org.apache.spark.sql.Row]): Hits = {
    var rddLeftLength = rddLeft.count()
    var rddRightLength = rddRight.count()
    var yLength = 0L
    var x: RDD[org.apache.spark.sql.Row] = null
    var y: RDD[org.apache.spark.sql.Row] = null

    if (rddLeftLength <= rddRightLength) {
      x = rddLeft
      yLength = rddRightLength
      y = rddRight
    } else {
      x = rddRight
      yLength = rddLeftLength
      y = rddLeft
    }
    var nclist = NCListBuilder.build(x)
    println("zbudowane nclist")
    var xWithIndices = x.map(elt => (elt(0).toString.toLong, elt(1).toString.toLong)).zipWithIndex().map { case (k, v) => (v, k) }
    var xStarts = xWithIndices.map(_._2._1).collect()
    var xEnds = xWithIndices.map(_._2._2).collect()

    var yWithIndices = y.map(elt => (elt(0).toString.toLong, elt(1).toString.toLong)).zipWithIndex().map { case (k, v) => (v, k) }

    var backpack: Backpack = null
    var nHit = 0
    var backpackHits = Array.empty[Int]
    var yHits = Array.empty[Int]
    for (i <- 0 until yLength.toInt) {
      backpack = Backpack(xStarts, xEnds, i, yWithIndices.lookup(i).map(_._1).head, yWithIndices.lookup(i).map(_._2).head, backpackHits)
      getYOverlapsFun(nclist, backpack)
      if(backpack.hits.length > nHit) {
        for (j <- nHit until backpack.hits.length ) {
          yHits :+= i
        }
        nHit = backpack.hits.length
        backpackHits = backpack.hits
      }
    }

    if (rddLeftLength <= rddRightLength)
      return Hits(backpack.hits, yHits)
    else
      return Hits(yHits, backpack.hits)
  }

  def getYOverlapsFun(topNcList: NCList, backpack: Backpack): Unit = {
    var walkingStack = mutable.Stack[NCListWalkingStack]()
    walkingStack.clear()

    var n = findLandingChild(topNcList, backpack)
    if (n < 0)
      return

    var ncList = moveToChild(topNcList, n, walkingStack)
    while (ncList != null) {
      var stackElt = peekNCListWalkingStackElt(walkingStack)
      var rgid = stackElt.parentNcList.rgidBuf(stackElt.n)
      breakable {
        if (backpack.xStarts(rgid) > backpack.maxXStart) {
          /* Skip all further siblings of 'nclist'. */
          ncList = moveToRightUncle(walkingStack)
          break //continue
        }

        reportHit(rgid, backpack)
        n = findLandingChild(ncList, backpack)
        /* Skip first 'n' or all children of 'nclist'. */
        ncList = if (n >= 0) moveToChild(ncList, n, walkingStack) else moveToRightSiblingOrUncle(ncList, walkingStack)
      }
    }
    println("childNclist")
  }

  def findLandingChild(ncList: NCList, backpack: Backpack): Int = {
    var nChildren = ncList.nChildren
    if (nChildren == 0)
      return -1;

    var n = intBsearch(ncList.rgidBuf, nChildren, backpack.xEnds, backpack.minXEnd)

    if (n >= nChildren)
      return -1;

    return n
  }

  def intBsearch(subset: Array[Int], subsetLen: Int, base: Array[Long], min: Long): Int = {
    /* Check first element. */
    var n1 = 0
    var b = base(subset(n1))
    if (b >= min)
      return n1

    /* Check last element. */
    var n2 = subsetLen - 1
    b = base(subset(n2))
    if (b < min)
      return subsetLen
    if (b == min)
      return n2

    /* Binary search.*/
    var n = (n1 + n2) / 2
    while (n != n1) {
      b = base(subset(n))
      if (b == min)
        return n
      if (b < min)
        n1 = n
      else
        n2 = n

      n = (n1 + n2) / 2
    }
    return n2
  }

  def moveToChild(parentNcList: NCList, n: Int, walkingStack: mutable.Stack[NCListWalkingStack]): NCList = {
    walkingStack.push(NCListWalkingStack(parentNcList, n))
    parentNcList.childrenBuf(n)
  }

  def peekNCListWalkingStackElt(walkingStack: mutable.Stack[NCListWalkingStack]): NCListWalkingStack = {
    walkingStack.top
  }

  def moveToRightUncle(walkingStack: mutable.Stack[NCListWalkingStack]): NCList = {
    var parentNcList = walkingStack.pop().parentNcList
    if (walkingStack.isEmpty)
      return null
    return moveToRightSiblingOrUncle(parentNcList, walkingStack)
  }

  def moveToRightSiblingOrUncle(ncList: NCList, walkingStack: mutable.Stack[NCListWalkingStack]): NCList = {
    var ncListLocal = ncList
    var stackEltPlusPlus: NCListWalkingStack = null

    do {
      var stackElt = walkingStack.pop()
      if ((stackElt.n+1) < stackElt.parentNcList.nChildren) {
        walkingStack.push(NCListWalkingStack(stackElt.parentNcList,stackElt.n+1))
        ncListLocal = stackElt.parentNcList.childrenBuf(stackElt.n+1)
        return ncListLocal
      } else {
        walkingStack.push(NCListWalkingStack(stackElt.parentNcList,stackElt.n+1))
        ncListLocal = stackElt.parentNcList
        walkingStack.pop()
      }
    } while (!walkingStack.isEmpty)
    return null
  }

  def reportHit(rgid: Int, backpack: Backpack) = {
    backpack.hits :+= rgid
  }
}

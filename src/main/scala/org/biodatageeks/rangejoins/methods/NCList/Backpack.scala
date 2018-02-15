package org.biodatageeks.rangejoins.NCList

case class Backpack[T](intervalArr: Array[(Interval[Int],T)], processedInterval: Interval[Int]) {

}

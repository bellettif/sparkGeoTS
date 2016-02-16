package main.scala.spatial

/**
 * Created by Praagya on 1/27/16.
 */

case class Location(x: Double, y: Double)

case class DataPoint(loc: Location, data: Any)

case class Boundary(centerX: Double, centerY: Double, delX: Double, delY: Double) {

  def contains(x: Double, y: Double): Boolean = {
    x < centerX + delX && x >= centerX - delX &&
      y < centerY + delY && y >= centerY - delY
  }

}

abstract class QuadTreeStructure(val b: Boundary, val locationCode: Int, val level: Int, val capacity: Int) {

  /*
  0 -> SW, 1 -> SE, 2 -> NW, 3 -> NE
   */
  var children: Option[Array[QuadTreeNode]] = None

  /*
  Array storing delta differences with neighbors
  Keys:
  0 -> E, 1 -> NE, 2 -> N, 3 -> NW,
  4 -> W, 5 -> SW, 6 -> S, 7 -> SE
  Values:
  0 -> adjacent quadrant is of same level)
  1 -> adjacent quadrant is of larger level (i.e. smaller than me)
  2 -> adjacent quadrant does not exist (i.e. i am at a border)
  3 -> diagonal
  -i -> adjacent quadrant is smaller level by -n (i.e. larger than me
   */
  val delta: Array[Int] = Array(2,2,2,2,2,2,2,2)

  def isLeaf = children.isDefined

  var nodeData = Vector[DataPoint]

  def getBucket(loc: Location): Option[QuadTreeStructure] = {
    if (isLeaf && b.contains(loc.x, loc.y)) {
      Some(this)
    } else {
      val candidate = children.get.find(_.b.contains(loc.x, loc.y))
      if (candidate.isEmpty) {
        None
      } else {
        candidate.get.getBucket(loc)
      }
    }
  }

}

object QuadTree {

  def apply(bucketSize: Int, xMin: Double, yMin: Double, xMax: Double, yMax: Double): QuadTreeRoot = {
    val centerX = xMin + ((xMax - xMin) / 2)
    val centerY = yMin + ((yMax - yMin) / 2)
    new QuadTreeRoot(Boundary(centerX, centerY, xMax - centerX, yMax - centerY), 0, 0, bucketSize)
  }

}

class QuadTreeRoot(b: Boundary, locationCode: Int, level: Int, capacity: Int) extends QuadTreeStructure(b, locationCode, level, capacity) {

  var leaves = Vector[QuadTreeNode]

  var depth: Int = 0

  /*

  extend(Seq[(Location, DataPoint)])

   */

  def getNode(queryLocationCode: Int, queryLevel: Int): Option[QuadTreeStructure] = {
    var loc: Int = queryLocationCode
    var i = 0
    var end: Boolean = false
    var q: Option[QuadTreeStructure] = Some(this)
    while (i < queryLevel && !end) {
      val tmp: QuadTreeNode = q.children(loc % 4)
      if (tmp == null) {
        end = true
      } else {
        loc = loc >> 2
        q = tmp
        i += 1
      }
    }
    q
  }

}

class QuadTreeNode(val root: QuadTreeRoot, b: Boundary, locationCode: Int, level: Int, capacity: Int) extends QuadTreeStructure(b, locationCode, level, capacity) {

  def getChildBoundaries(): Array[Boundary] = {
    Array(
      Boundary(b.centerX - (b.delX / 2.0), b.centerY - (b.delY / 2.0), b.delX / 2.0, b.delY / 2.0), // SW
      Boundary(b.centerX - (b.delX / 2.0), b.centerY + (b.delY / 2.0), b.delX / 2.0, b.delY / 2.0), // SE
      Boundary(b.centerX + (b.delX / 2.0), b.centerY - (b.delY / 2.0), b.delX / 2.0, b.delY / 2.0), // NW
      Boundary(b.centerX + (b.delX / 2.0), b.centerY + (b.delY / 2.0), b.delX / 2.0, b.delY / 2.0) // NE
    )
  }


}


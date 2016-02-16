package main.scala.spatial


import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.pow

/**
 * Created by Praagya on 1/27/16.
 */

case class Location(x: Double, y: Double)

case class DataPoint(loc: Location, data: Any)

case class Boundary(centerX: Double, centerY: Double, delX: Double, delY: Double) {

  def contains(loc: Location): Boolean = {
    loc.x < centerX + delX && loc.x >= centerX - delX &&
      loc.y < centerY + delY && loc.y >= centerY - delY
  }

}

object QuadTree {

  def apply(bucketSize: Int, xMin: Double, yMin: Double, xMax: Double, yMax: Double): QuadTreeRoot = {
    val centerX = xMin + ((xMax - xMin) / 2)
    val centerY = yMin + ((yMax - yMin) / 2)
    new QuadTreeRoot(Boundary(centerX, centerY, xMax - centerX, yMax - centerY), bucketSize, 1)
  }

}

abstract class QuadTreeStructure(val boundary: Boundary, val depth: Int) {

  /*
  0 -> SW, 1 -> SE, 2 -> NW, 3 -> NE
   */
  var children: Option[Array[QuadTreeNode]] = None

  def isLeaf = children.isEmpty

  var nodeData = new ArrayBuffer[DataPoint]()

  def getLeafNode(loc: Location): Option[QuadTreeStructure] = {
    if (isLeaf && boundary.contains(loc)) {
      Some(this)
    } else {
      val candidate = children.get.find(_.boundary.contains(loc))
      if (candidate.isEmpty) {
        None
      } else {
        candidate.get.getLeafNode(loc)
      }
    }
  }

  def split(): Array[QuadTreeNode] = {
    val newChildren = Array(
      Boundary(boundary.centerX - (boundary.delX / 2.0), boundary.centerY - (boundary.delY / 2.0), boundary.delX / 2.0, boundary.delY / 2.0), // SW
      Boundary(boundary.centerX + (boundary.delX / 2.0), boundary.centerY - (boundary.delY / 2.0), boundary.delX / 2.0, boundary.delY / 2.0), // SE
      Boundary(boundary.centerX - (boundary.delX / 2.0), boundary.centerY + (boundary.delY / 2.0), boundary.delX / 2.0, boundary.delY / 2.0), // NW
      Boundary(boundary.centerX + (boundary.delX / 2.0), boundary.centerY + (boundary.delY / 2.0), boundary.delX / 2.0, boundary.delY / 2.0) // NE
    ).map(bnd => new QuadTreeNode(bnd, depth + 1, nodeData.filter(pt => bnd.contains(pt.loc))))
    nodeData.clear()
    children = Some(newChildren)
    newChildren
  }

}

class QuadTreeNode(boundary: Boundary, depth: Int, initialData: Seq[DataPoint]) extends QuadTreeStructure(boundary, depth) {

  nodeData ++= initialData

}

class QuadTreeRoot(boundary: Boundary, bucketSize: Int, depth: Int) extends QuadTreeStructure(boundary, depth) {

  var leaves = new ArrayBuffer[QuadTreeStructure]()
  leaves += this

  var maxDepth: Int = 1

  def add(pt: DataPoint): Unit = {
    val bucket = getLeafNode(pt.loc)
    if (bucket.isDefined) {
      bucket.get.nodeData += pt
      if (bucket.get.nodeData.length > bucketSize) {
        leaves -= bucket.get
        val newChildren = bucket.get.split()
        leaves ++= newChildren
        if (newChildren(0).depth > maxDepth) {
          maxDepth = newChildren(0).depth
        }
      }
    }
  }

  def getNeighbors(loc: Location): Array[QuadTreeStructure] = {
    val q = getLeafNode(loc)
    var ret = new mutable.HashSet[QuadTreeStructure]()
    if (q.isDefined) {
      ret += q.get
      val depthDifference = if (maxDepth - q.get.depth < 0) 0 else maxDepth - q.get.depth
      val (xOut, yOut) = (q.get.boundary.delX / pow(2, depthDifference), q.get.boundary.delY / pow(2, depthDifference))
      val xRange = (q.get.boundary.centerX - q.get.boundary.delX - xOut) to (q.get.boundary.centerX + q.get.boundary.delX + xOut) by (q.get.boundary.delX / pow(2, depthDifference - 1))
      val yRange = (q.get.boundary.centerY - q.get.boundary.delY - yOut) to (q.get.boundary.centerY + q.get.boundary.delY + yOut) by (q.get.boundary.delY / pow(2, depthDifference - 1))
      val neighborCoordinates = (for (i <- xRange; j <- yRange) yield Location(i, j)).filter(loc => !q.get.boundary.contains(loc) && boundary.contains(loc))
      ret ++= neighborCoordinates.flatMap(getLeafNode)
    }
    ret.toArray
  }

}

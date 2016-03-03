package main.scala.spatial


import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.pow
import scala.reflect.ClassTag

/**
 * Created by Praagya on 1/27/16.
 */

case class Boundary(centerX: Double, centerY: Double, delX: Double, delY: Double) {

  def contains(x: Double, y: Double): Boolean = {
    x < centerX + delX && x >= centerX - delX &&
      y < centerY + delY && y >= centerY - delY
  }

}

case class kBoundary(dim: Int, centerVec: Seq[Double], delVec: Seq[Double]) {
  assert(centerVec.length == delVec.length)
  assert(dim == centerVec.length)
  assert(dim > 0)

  def contains(vec: Seq[Double]): Boolean = {
    assert(vec.length == dim)
    (0 until dim).forall(i => ((centerVec(i) - delVec(i)) <= vec(i)) && (vec(i) < (centerVec(i) + delVec(i))))
  }

  def generatePlusMinusOnes(n: Int) = generateHelper(n - 1, List(List(1.0), List(-1.0)))

  def generateHelper(n: Int, soFar: List[List[Double]]): Array[Array[Double]] = {
    if (n < 1) {
      soFar.map(_.toArray).toArray
    } else {
      generateHelper(n - 1, soFar.map(_ :+ 1.0) ++ soFar.map(_ :+ -1.0))
    }
  }

  def split(): Array[kBoundary] = {
    val newDelVector = delVec.map(_ / 2.0)
    val newCenterVectors = generatePlusMinusOnes(dim).map(pmOnes => pmOnes.zip(centerVec).map(t => t._1 * t._2))
    newCenterVectors.map(kBoundary(dim, _, newDelVector))
  }

}

object QuadTree {

  def apply[DataT: ClassTag](bucketSize: Int, xMin: Double, yMin: Double, xMax: Double, yMax: Double): QuadTreeRoot[DataT] = {
    val centerX = xMin + ((xMax - xMin) / 2)
    val centerY = yMin + ((yMax - yMin) / 2)
    new QuadTreeRoot(Boundary(centerX, centerY, xMax - centerX, yMax - centerY), bucketSize, 1)
  }

}

abstract class QuadTreeStructure[DataT : ClassTag](val boundary: Boundary, val depth: Int) {

  var children: Option[Array[QuadTreeNode[DataT]]] = None

  def isLeaf = children.isEmpty

  var nodeData = new ArrayBuffer[(Double, Double, DataT)]()

  def getLeafNode(x: Double, y: Double): Option[QuadTreeStructure[DataT]] = {
    if (isLeaf && boundary.contains(x, y)) {
      Some(this)
    } else {
      if (children.isDefined) {
        children.get.find(_.boundary.contains(x, y)) match {
          case None => None
          case Some(leaf) => leaf.getLeafNode(x, y)
        }
      } else {
        None
      }
    }
  }

  def split(): Array[QuadTreeNode[DataT]] = {
    val newChildren = Array(
      Boundary(boundary.centerX - (boundary.delX / 2.0), boundary.centerY - (boundary.delY / 2.0), boundary.delX / 2.0, boundary.delY / 2.0),
      Boundary(boundary.centerX + (boundary.delX / 2.0), boundary.centerY - (boundary.delY / 2.0), boundary.delX / 2.0, boundary.delY / 2.0),
      Boundary(boundary.centerX - (boundary.delX / 2.0), boundary.centerY + (boundary.delY / 2.0), boundary.delX / 2.0, boundary.delY / 2.0),
      Boundary(boundary.centerX + (boundary.delX / 2.0), boundary.centerY + (boundary.delY / 2.0), boundary.delX / 2.0, boundary.delY / 2.0)
    ).map(bnd => new QuadTreeNode[DataT](bnd, depth + 1, nodeData.filter(pt => bnd.contains(pt._1, pt._2))))
    nodeData.clear()
    children = Some(newChildren)
    newChildren
  }

}

class QuadTreeNode[DataT : ClassTag](boundary: Boundary, depth: Int, initialData: Seq[(Double, Double, DataT)]) extends QuadTreeStructure[DataT](boundary, depth) {

  nodeData ++= initialData

}

class QuadTreeRoot[DataT: ClassTag](boundary: Boundary, bucketSize: Int, depth: Int) extends QuadTreeStructure[DataT](boundary, depth) {

  var leaves = new ArrayBuffer[QuadTreeStructure[DataT]]()
  leaves += this

  var maxDepth: Int = 1

  def add(pt: (Double, Double, DataT)): Unit = {
    val bucket = getLeafNode(pt._1, pt._2)
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

  def getNeighbors(x: Double, y: Double): Array[QuadTreeStructure[DataT]] = {
    val q = getLeafNode(x, y)
    var ret = new mutable.HashSet[QuadTreeStructure[DataT]]()
    if (q.isDefined) {
      ret += q.get
      val depthDifference = if (maxDepth - q.get.depth < 0) 0 else maxDepth - q.get.depth
      val (xOut, yOut) = (q.get.boundary.delX / pow(2, depthDifference), q.get.boundary.delY / pow(2, depthDifference))
      val xRange = (q.get.boundary.centerX - q.get.boundary.delX - xOut) to (q.get.boundary.centerX + q.get.boundary.delX + xOut) by (q.get.boundary.delX / pow(2, depthDifference - 1))
      val yRange = (q.get.boundary.centerY - q.get.boundary.delY - yOut) to (q.get.boundary.centerY + q.get.boundary.delY + yOut) by (q.get.boundary.delY / pow(2, depthDifference - 1))
      val neighborCoordinates = (for (i <- xRange; j <- yRange) yield (i, j)).filter(loc => !q.get.boundary.contains(loc._1, loc._2) && boundary.contains(loc._1, loc._2))
      ret ++= neighborCoordinates.flatMap(loc => getLeafNode(loc._1, loc._2))
    }
    ret.toArray
  }

}

package main.scala.spatial

/**
 * Created by Praagya on 1/27/16.
 */


case class Location(x: Double, y: Double)

case class TData(i: Double, loc: Location)

class QuadTree(val nodeData: List[Double], val parent: QuadTree, val children: Array[QuadTree]) {

  def isLeaf = children == null

  def getAllLeaves: List[QuadTree] = {
    var ret = List[QuadTree]()
    if (isLeaf) {
      ret = ret ++ List[QuadTree](this)
    } else {
      ret = ret ++ children(0).getAllLeaves ++ children(1).getAllLeaves ++ children(2).getAllLeaves ++ children(3).getAllLeaves
    }
    ret.filter(_.nodeData.nonEmpty)
  }

  def getBucket(i: Double): List[QuadTree] = getAllLeaves.filter(_.nodeData.contains(i))

}

object QuadTreeHelper {

  def construct(tdata: List[TData], minBucketSize: Int) = {
      val xValues = tdata.map(_.loc.x)
      val yValues = tdata.map(_.loc.y)
      constructHelper(tdata, minBucketSize, null, xValues.min, xValues.max, yValues.min, yValues.max)
    }

  def constructHelper(tdata: List[TData], minBucketSize: Int, parent: QuadTree, xMin: Double, xMax: Double, yMin: Double, yMax: Double): QuadTree = {
    if (tdata.length <= minBucketSize) {
      new QuadTree(tdata.map(_.i), parent, null)
    } else {
      val xMid = xMin + (xMax - xMin) / 2
      val yMid = yMin + (yMax - yMin) / 2
      val northWest = tdata.filter((t: TData) => t.loc.x <= xMid && t.loc.y > yMid)
      val northEast = tdata.filter((t: TData) => t.loc.x > xMid && t.loc.y > yMid)
      val southWest = tdata.filter((t: TData) => t.loc.x <= xMid && t.loc.y <= yMid)
      val southEast = tdata.filter((t: TData) => t.loc.x > xMid && t.loc.y <= yMid)
      val myNode = new QuadTree(null, parent, new Array[QuadTree](4))
      myNode.children(0) = constructHelper(southWest, minBucketSize, myNode, xMin, xMid, yMin, yMid)
      myNode.children(1) = constructHelper(southEast, minBucketSize, myNode, xMid, xMax, yMin, yMid)
      myNode.children(2) = constructHelper(northWest, minBucketSize, myNode, xMin, xMid, yMid, yMax)
      myNode.children(3) = constructHelper(northEast, minBucketSize, myNode, xMid, xMax, yMid, yMax)
      myNode
    }
  }

}

/*

case class Boundary(centerX: Double, centerY: Double, delX: Double, delY: Double) {

  def contains(x: Double, y: Double): Boolean = {
    x < centerX + delX && x >= centerX - delX &&
      y < centerY + delY && y >= centerY - delY
  }

}

object QuadTree {

  def apply(centerX: Double, centerY: Double, delX: Double, delY: Double): Unit = {

  }

}



case class Location(x: Double, y: Double)

case class TData(t: Double, loc: Location, obs: Double)

case class QuadTreeNode(locationCode: Int, obsData: List[(Double, Double)])

object QuadTreeHelper {

  def tDataListToQTree(tdata: List[TData], locationCode: String): QuadTreeNode = {
    QuadTreeNode(locationCode.toInt, tdata.map(td => (td.t, td.obs)))
  }

  def construct(tdata: List[TData], minBucketSize: Int): List[QuadTreeNode] = {
    if (tdata.length < minBucketSize) {
      List(tDataListToQTree(tdata, "0"))
    } else {
      val xValues = tdata.map(_.loc.x)
      val yValues = tdata.map(_.loc.y)
      pad(constructHelper(tdata, minBucketSize, "", xValues.min, xValues.max, yValues.min, yValues.max))
    }
  }

  def pad(qtNodes: List[QuadTreeNode]): List[QuadTreeNode] = {
    val resolution = qtNodes.map(_.locationCode.toString.length).max
    qtNodes.map(node => padZeroes(node, resolution))
  }

  def padZeroes(node: QuadTreeNode, r: Int): QuadTreeNode = {

  }

  def constructHelper(tdata: List[TData], minBucketSize: Int, locCodeSoFar: String, xMin: Double, xMax: Double, yMin: Double, yMax: Double): List[QuadTreeNode] = {
    var ret = List[QuadTreeNode]()
    if (tdata.length <= minBucketSize) {
      List(tDataListToQTree(tdata, locCodeSoFar))
    } else {
      val xMid = xMin + (xMax - xMin) / 2
      val yMid = yMin + (yMax - yMin) / 2
      val northWest = tdata.filter((t: TData) => t.loc.x <= xMid && t.loc.y > yMid)
      if (northWest.nonEmpty) {
        ret = ret ++ constructHelper(northWest, minBucketSize, locCodeSoFar + "2", xMin, xMid, yMid, yMax)
      }
      val northEast = tdata.filter((t: TData) => t.loc.x > xMid && t.loc.y > yMid)
      if (northEast.nonEmpty) {
        ret = ret ++ constructHelper(northEast, minBucketSize, locCodeSoFar + "3", xMid, xMax, yMid, yMax)
      }
      val southWest = tdata.filter((t: TData) => t.loc.x <= xMid && t.loc.y <= yMid)
      if (southWest.nonEmpty) {
        ret = ret ++ constructHelper(southWest, minBucketSize, locCodeSoFar + "0", xMin, xMid, yMin, yMid)
      }
      val southEast = tdata.filter((t: TData) => t.loc.x > xMid && t.loc.y <= yMid)
      if (southEast.nonEmpty) {
        ret = ret ++ constructHelper(southEast, minBucketSize, locCodeSoFar + "1", xMid, xMax, yMin, yMid)
      }
      ret
    }
  }

}
*/
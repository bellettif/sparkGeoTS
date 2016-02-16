package test.scala

/**
 * Created by Praagya on 2/10/16.
 */

import main.scala.spatial._
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class TestQuadTree extends FlatSpec with Matchers {

  def randDouble(rMin: Double, rMax: Double) = rMin + ((rMax - rMin) * Random.nextDouble())

  def generateRandomQuadTree(nSamples: Int, xMin: Double, yMin: Double, xMax: Double, yMax: Double, bucketSize: Int): QuadTreeRoot = {
    val q = QuadTree(bucketSize, xMin, yMin, xMax, yMax)
    Seq.fill(nSamples)(DataPoint(Location(randDouble(xMin, xMax), randDouble(yMin, yMax)), Random.nextDouble())).map(q.add)
    q
  }

  def computeDepth(q: QuadTreeStructure): Int = {
    if (q.isLeaf) {
      1
    } else {
      1 + q.children.get.map(computeDepth(_)).max
    }
  }

  val numSamples = 10000
  val bucketSize = 5
  val sampleWidth = 100

  val q = generateRandomQuadTree(numSamples, -sampleWidth, -sampleWidth, sampleWidth, sampleWidth, bucketSize)

  "All randomly generated values" should "have successfully been added to the QuadTree" in {
    q.leaves.map(_.nodeData.length).sum should be (numSamples)
  }

  "All QuadTree buckets" should "not exceed capacity" in {
    val f = false
    q.leaves.map(_.nodeData.length).forall(_ > bucketSize) should be (f)
  }

  "Depth of the tree" should "be computed and updated correctly" in {
    q.maxDepth should be (computeDepth(q))
  }

  "getNeighbors" should "return all equal sized neighbors" in {
    val nq = generateRandomQuadTree(0, -2, -2, 2, 2, 1)
    val pts = List((-1, -1, 0), (1, -1, 1), (-1, 1, 2), (1, 1, 3)).map(t => DataPoint(Location(t._1, t._2), t._3))
    pts.foreach(nq.add)
    nq.getNeighbors(Location(-1, 1)).length should be (4)
  }

  "getNeighbors" should "return proper neighbors in case of differing depths" in {
    val nq = generateRandomQuadTree(0, -4, -4, 4, 4, 1)
    val pts = List(
      (-3.0, 3.0, 1), (-1.0, 3.0, 2), (1.0, 3.0, 3), (3.0, 3.0, 4), (-3.0, 1.0, 5), (-1.0, 1.0, 6),
      (0.5, 1.5, 7), (1.5, 1.5, 8), (0.5, 0.5, 9), (1.5, 0.5, 10), (3.0, 1.0, 11),
      (-2.0, -2.0, 12), (0.25, -0.25, 13), (0.75, -0.25, 14), (0.25, -0.75, 15), (0.75, -0.75, 16),
      (1.5, -0.5, 17), (0.5, -1.5, 18), (1.5, -1.5, 19), (3.0, -1.0, 20), (1.0, -3.0, 21), (3.0, -3.0, 22)
    ).map(t => DataPoint(Location(t._1, t._2), t._3))
    pts.foreach(nq.add)
    nq.getNeighbors(Location(-1, 1)).length should be (9)
  }

}

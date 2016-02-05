package test.scala

/**
 * Created by Praagya on 2/3/16.
 */

import main.scala.spatial.{QuadTreeHelper, Location, TData}
import org.scalatest.{FlatSpec, Matchers}

class TestQuadTreeNode extends FlatSpec with Matchers {

  /*
  "QuadTree construction" should "assign proper location codes" in {
    val sampleData = List((-3,3), (-2, 3), (-1, 1), (-3, -2), (-1, -2),
                          (-3, -4), (-1, -4), (3, -4), (2,2), (4,2))
                          .map(xy => TData(1, Location(xy._1, xy._2), 5))
    /*val sampleData = List((-10,-10, 1), (5,5,2), (1, -1,3), (-1, 1,4))
                      .map(xy => TData(1, Location(xy._1, xy._2), xy._3))*/
    println(QuadTreeHelper.construct(sampleData, 5))
  }*/

  "QuadTree construction" should "properly partition data spatially" in {
    val sampleData = List((1, 1 ,1), (2, -1, -1), (3, 1, -1), (4, -1, 1),
      (5, 1, 2), (6, 2, 2), (7, 2, 3))
                      .map(t => TData(t._1, Location(t._2,t. _3)))
    val q = QuadTreeHelper.construct(sampleData, 4)
    val myNodeData = q.getAllLeaves.map(_.nodeData)
  }

}

package main.scala.showcase

import breeze.plot.Figure
import main.scala.spatial.QuadTree
import breeze.plot.image

import scala.collection.mutable.ArrayBuffer

/**
 * Created by praagya on 2/26/16.
 */

object QuadTreeShowCase {

  def countBucket[DataT](a: ArrayBuffer[(Double, Double, DataT)]): Double = a.length.toDouble

  /*
  Reads in a csv file of new york taxicab data in the following format:
  {datetime},{latitude},{longitude}
  Visualizes the spatial buckets using spatial.QuadTree
   */
  def main (args: Array[String]): Unit = {
    val (lonMin, latMin, lonMax, latMax) = (-74.273294, 40.493642, -73.656686, 40.921499)
    val bucketSize = 50
    val q = QuadTree[Int](bucketSize, lonMin, latMin, lonMax, latMax)
    val taxiData = scala.io.Source.fromFile("src/main/scala/showcase/uberData.csv")
    for (tData <- taxiData.getLines()) {
      val row = tData.split(",").map(_.trim)
      q.add((row(2).toDouble, row(1).toDouble, 1))
    }
    println("QuadTree constructed")
    val fig = Figure()
    val test = image(q.toMatrix(countBucket))
    fig.subplot(0) += image(q.toMatrix(countBucket))
    fig.saveas("quadtree.png")
  }

}

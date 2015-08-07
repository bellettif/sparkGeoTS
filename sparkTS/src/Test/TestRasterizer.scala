package test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.joda.time.DateTime

import org.scalatest._

import breeze.linalg._

import geoTsRDD.{Aggregator, Rasterizer, LocEvent}

import scala.math._

/**
 * Created by Francois Belletti on 6/22/15.
 */
class TestRasterizer extends FlatSpec with Matchers{

  "A rasterizer" should "conserve sum of elements with sum aggregators" in {

    val nSamples: Int = 1e4.toInt
    val nColumns: Int = 4

    val rawData: Seq[DenseVector[Double]] = (0 until nSamples)
      .map(_ => DenseVector.rand[Double](nColumns))

    val conf = new SparkConf().setAppName("TestRasterizer").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawDataRdd = sc.parallelize(rawData)


    val eventRdd = rawDataRdd
      .map(x => ((x.apply(0), x.apply(1), x.apply(2)), x.apply(3)))
      .map({ case (txy, v) => new LocEvent[(Double, Double, Double), Double](txy, v) })

    val rasterizer = new Rasterizer[
      (Double, Double, Double),
      (Long, Long, Long),
      Double,
      Double](
        loc => {
          val xIdx: Long = floor((loc._1 - 0.0) / 0.1).toLong
          val yIdx: Long = floor((loc._2 - 0.0) / 0.1).toLong
          val tIdx: Long = floor((loc._3 - 0.0) / 0.1).toLong
          (xIdx, yIdx, tIdx)
        },
        new Aggregator[Double, Double](
          x => x,
          _ + _,
          _ + _
        ),
        {case (x, y) => (x, y)},
        eventRdd)

    val sumAllValues = rawData.map(_.apply(3)).reduce(_ + _)

    val sumRasters = rasterizer.rasters
        .map({case (x, y) => y})
        .reduce(_ + _)

      sumAllValues should be (sumRasters +- 0.01)
    }

}

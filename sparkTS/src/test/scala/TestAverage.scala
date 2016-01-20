package test.scala

/**
 * Created by Francois Belletti on 8/17/15.
 */

import breeze.linalg.DenseVector
import breeze.numerics.sqrt
import breeze.stats.distributions.{Gaussian, Uniform}
import main.scala.overlapping.analytics._
import main.scala.overlapping.containers.{SingleAxisVectTS, SingleAxisVectTSConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}


class TestAverage extends FlatSpec with Matchers{

  val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
  val sc    = new SparkContext(conf)

  "Mean estimation " should " properly work for series of ones" in {

    val nColumns = 10
    val h = 6
    val nSamples = 80000L
    val deltaTMillis = 1L
    val deltaT = new DateTime(deltaTMillis)
    val paddingMillis = new DateTime(deltaTMillis * 10)
    val nPartitions   = 8
    val config = new SingleAxisVectTSConfig(nSamples, deltaT, paddingMillis, paddingMillis, nColumns)

    val rawTS = Surrogate.generateOnes(
      nColumns,
      nSamples.toInt,
      deltaT,
      sc)

    val (timeSeries, _) = SingleAxisVectTS(nPartitions, config, rawTS)

    val average = Average(timeSeries)

    average.length should be (nColumns)

    for(i <- 0 until nColumns){

      average(i) should be (1.0)

    }

  }

  it should " properly work for a white noise time series" in {

    val nColumns = 10
    val h = 6
    val nSamples = 80000L
    val deltaTMillis = 1L
    val deltaT = new DateTime(deltaTMillis)
    val paddingMillis = new DateTime(deltaTMillis * 10)
    val nPartitions   = 8
    val config = new SingleAxisVectTSConfig(nSamples, deltaT, paddingMillis, paddingMillis, nColumns)

    val rawTS: RDD[(DateTime, DenseVector[Double])] = Surrogate.generateWhiteNoise(
      nColumns,
      nSamples.toInt,
      deltaT,
      Gaussian(0.0, 1.0),
      DenseVector.ones[Double](nColumns),
      sc)

    val (timeSeries, _) = SingleAxisVectTS(nPartitions, config, rawTS)

    val average = Average(timeSeries)

    average.length should be (nColumns)

    for(i <- 0 until nColumns){

      average(i) should be (0.0 +- 5.0 / sqrt(nSamples.toDouble))

    }

  }


}
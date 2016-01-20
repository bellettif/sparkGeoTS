package test.scala

/**
 * Created by Francois Belletti on 8/17/15.
 */

import breeze.linalg.{DenseVector, min, sum}
import breeze.numerics._
import breeze.stats.distributions.Uniform
import main.scala.overlapping.analytics._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}


class TestSurrogate extends FlatSpec with Matchers{

  val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
  val sc    = new SparkContext(conf)

  "Surrogate data generator " should " properly generate a collection of ones" in {

    val nColumns = 10
    val nSamples = 80000L
    val deltaT = new DateTime(4L)

    val rawTS = Surrogate.generateOnes(
      nColumns,
      nSamples.toInt,
      deltaT,
      sc)

    val result = rawTS.collect

    for (((t, x), i) <- result.zipWithIndex) {
      t.getMillis should be(deltaT.getMillis * i)

      for (j <- 0 until nColumns) {
        x(j) should be(1.0)
      }

    }

  }

  it should " properly generate white noise" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val deltaT        = new DateTime(4L)

    val b = 0.5
    val a = -0.5

    val rawTS = Surrogate.generateWhiteNoise(
      nColumns,
      nSamples.toInt,
      deltaT,
      Uniform(a, b),
      DenseVector.ones[Double](nColumns),
      sc)

    val result = rawTS.collect

    for(((t, x), i) <- result.zipWithIndex){
      t.getMillis should be (deltaT.getMillis * i)

      for(j <- 0 until nColumns){
        x(j) should be <= b
        x(j) should be >= a
      }

    }

    val mean = result.map(_._2).reduce(_ + _) / nSamples.toDouble

    for(j <- 0 until nColumns){
      mean(j) should be (0.0 +- 5.0 * (b - a) / sqrt(12.0 * nSamples.toDouble))
    }

    val variance = result.map(x => x._2 :* x._2).reduce(_ + _) / nSamples.toDouble

    for(j <- 0 until nColumns){
      variance(j) should be (1.0 / 12.0 * (b - a) * (b - a) +- 10.0 / sqrt(nSamples.toDouble))
    }

  }




}
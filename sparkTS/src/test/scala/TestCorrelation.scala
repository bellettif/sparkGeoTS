package test.scala

/**
 * Created by Francois Belletti on 8/17/15.
 */

import breeze.linalg.DenseVector
import breeze.numerics._
import breeze.stats.distributions.Gaussian
import main.scala.overlapping.analytics._
import main.scala.overlapping.containers.{SingleAxisVectTS, SingleAxisVectTSConfig}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}


class TestCorrelation extends FlatSpec with Matchers{

  val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
  val sc    = new SparkContext(conf)

  "Correlation estimation " should " properly work for series of white noise" in {

    val nColumns = 10
    val h = 6
    val nSamples = 80000L
    val deltaTMillis = 1L
    val deltaT = new DateTime(deltaTMillis)
    val paddingMillis = new DateTime(deltaTMillis * 10)
    val nPartitions   = 1
    val config = new SingleAxisVectTSConfig(nSamples, deltaT, paddingMillis, paddingMillis, nColumns)

    val rawTS = Surrogate.generateWhiteNoise(
      nColumns,
      nSamples.toInt,
      deltaT,
      Gaussian(0.0, 2.0),
      DenseVector.ones[Double](nColumns),
      sc)

    val (timeSeries, _) = SingleAxisVectTS(nPartitions, config, rawTS)

    val crossCorrelations = CrossCorrelation(timeSeries, h)

    crossCorrelations should have size (2 * h + 1)

    for(lagIdx <- 0 until (2 * h - 1)){

      val cov = crossCorrelations(lagIdx)

      cov.rows should be (nColumns)
      cov.cols should be (nColumns)

      if(lagIdx < h){

        for(i <- 0 until nColumns){
          for(j <- 0 until nColumns){
            cov(i, j) should be (crossCorrelations(2 * h - lagIdx)(j, i))
            cov(i, j) should be (0.0 +- 4.0 / sqrt(nSamples.toDouble))
          }
        }

      }else if(lagIdx == h){

        for(i <- 0 until nColumns){
          for(j <- 0 until nColumns){
            cov(i, j) should be (cov(j, i))
            if(i != j) {
              cov(i, j) should be(0.0 +- 4.0 / sqrt(nSamples.toDouble))
            }else{
              cov(i, j) should be(1.0 +- 4.0 / sqrt(nSamples.toDouble))
            }
          }
        }

      }else{

        for(i <- 0 until nColumns){
          for(j <- 0 until nColumns){
            cov(i, j) should be (crossCorrelations(h - (lagIdx - h))(j, i))
            cov(i, j) should be (0.0 +- 4.0 / sqrt(nSamples.toDouble))
          }
        }

      }

    }

  }

  it should " properly work a MA(1) time series" in {

    val nColumns = 10
    val h = 6
    val nSamples = 80000L
    val deltaTMillis = 1L
    val deltaT = new DateTime(deltaTMillis)
    val paddingMillis = new DateTime(deltaTMillis * 10)
    val nPartitions   = 1
    val config = new SingleAxisVectTSConfig(nSamples, deltaT, paddingMillis, paddingMillis, nColumns)

    val theta = 0.5
    val sigma = 2.0

    val rawTS = Surrogate.generateMA(
      Array.fill{nColumns}(DenseVector.ones[Double](1) * theta),
      nColumns,
      nSamples.toInt,
      deltaT,
      Gaussian(0.0, 2.0),
      DenseVector.ones[Double](nColumns),
      sc)

    val (timeSeries, _) = SingleAxisVectTS(nPartitions, config, rawTS)

    val crossCorrelations = CrossCorrelation(timeSeries, h)

    crossCorrelations should have size (2 * h + 1)

    for(lagIdx <- 0 until (2 * h - 1)){

      val cov = crossCorrelations(lagIdx)

      cov.rows should be (nColumns)
      cov.cols should be (nColumns)

      if(lagIdx < h - 1){

        for(i <- 0 until nColumns){
          for(j <- 0 until nColumns){
            cov(i, j) should be (crossCorrelations(2 * h - lagIdx)(j, i))
            cov(i, j) should be (0.0 +- 4.0 / sqrt(nSamples.toDouble))
          }
        }

      }else if(lagIdx == h - 1){

        for(i <- 0 until nColumns){
          for(j <- 0 until nColumns){
            cov(i, j) should be (crossCorrelations(h + 1)(j, i))
            if(i != j) {
              cov(i, j) should be(0.0 +- 4.0 / sqrt(nSamples.toDouble))
            }else{
              cov(i, j) should be((theta / (1 + theta * theta)) +- 4.0 / sqrt(nSamples.toDouble))
            }
          }
        }

      }else if(lagIdx == h){

        for(i <- 0 until nColumns){
          for(j <- 0 until nColumns){
            cov(i, j) should be (cov(j, i))
            if(i != j) {
              cov(i, j) should be(0.0 +- 4.0 / sqrt(nSamples.toDouble))
            }else{
              cov(i, j) should be(1.0 +- 4.0 / sqrt(nSamples.toDouble))
            }
          }
        }

      }else if(lagIdx == h + 1){

        for(i <- 0 until nColumns){
          for(j <- 0 until nColumns){
            cov(i, j) should be (crossCorrelations(h - 1)(j, i))
            if(i != j) {
              cov(i, j) should be(0.0 +- 4.0 / sqrt(nSamples.toDouble))
            }else{
              cov(i, j) should be((theta / (1 + theta * theta)) +- 4.0 / sqrt(nSamples.toDouble))
            }
          }
        }

      }else{

        for(i <- 0 until nColumns){
          for(j <- 0 until nColumns){
            cov(i, j) should be (crossCorrelations(h - (lagIdx - h))(j, i))
            cov(i, j) should be (0.0 +- 4.0 / sqrt(nSamples.toDouble))
          }
        }

      }

    }

  }


}
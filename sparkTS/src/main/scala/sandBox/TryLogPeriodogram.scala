package main.scala.sandBox

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.numerics.abs
import breeze.plot._
import breeze.stats.distributions.Gaussian
import main.scala.overlapping.containers._
import main.scala.overlapping.timeSeries._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TryLogPeriodogram {

  implicit def signedDistMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

  implicit def signedDistLong = (t1: Long, t2: Long) => (t2 - t1).toDouble

  def main(args: Array[String]): Unit = {

    val d = 3
    val N = 100000L
    val paddingMillis = 1000L
    val deltaTMillis = 1L
    val nPartitions = 8

    implicit val config = TSConfig(deltaTMillis, d, N, paddingMillis.toDouble)

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)

    val ARCoeffs = Array(
      DenseMatrix((0.80, 0.0, 0.0), (-0.80, 0.0, 0.0), (0.0, 0.0, 0.0))//,
      //DenseMatrix((0.12, 0.0, 0.0), (0.0, 0.08, 0.0), (0.0, 0.0, 0.45)),
      //DenseMatrix((-0.08, 0.0, 0.0), (0.0, 0.05, 0.0), (0.0, 0.0, 0.0))
    )

    val maxGain = Stability(ARCoeffs)
    if(maxGain > 1.0){
      println("Model is unstable (non causal) with maximum gain = " + maxGain)
    }else{
      println("Model is stable (causal) with maximum gain = " + maxGain)
    }

    val noiseMagnitudes = DenseVector.ones[Double](d)

    val rawTS = Surrogate.generateVAR(
      ARCoeffs,
      d,
      N.toInt,
      deltaTMillis,
      Gaussian(0.0, 1.0),
      noiseMagnitudes,
      sc)

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    overlappingRDD.persist()

    val h = 100
    val logper = LogPeriodogram(overlappingRDD, h)

    val f = Figure()

    var plotIndex = 0

    for(i <- 0 until d){
      val p = f.subplot(d, 1, plotIndex)

      plotIndex += 1

      p.ylabel = "sensor " + i
      p.xlabel = "time (ms)"

      p += plot(linspace(0.0,1.0, 2 * h + 1 - 20), logper(i, 10 until 201 - 10).t.toArray.map(x => abs(x)))

    }

    overlappingRDD.unpersist()

  }
}
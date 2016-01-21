package test.scala

/**
 * Created by Francois Belletti on 8/17/15.
 */

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.numerics._
import breeze.stats.distributions.Gaussian
import main.scala.overlapping.analytics._
import main.scala.overlapping.containers.{SingleAxisVectTS, SingleAxisVectTSConfig}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}


class TestVARModel extends FlatSpec with Matchers{

  val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
  val sc    = new SparkContext(conf)

  "VARModel estimation " should " properly retrieve model parameters" in {

    val nSamples = 1000000L
    val d = 3
    val deltaTMillis = 1L
    val deltaT = new DateTime(deltaTMillis) // 5 minutes
    val paddingMillis = new DateTime(deltaTMillis * 10)
    val nPartitions   = 8
    val config = new SingleAxisVectTSConfig(nSamples, deltaT, paddingMillis, paddingMillis, d)

    val p = 4

    val actualParams: Array[DenseMatrix[Double]] = Array.fill(p){DenseMatrix.rand[Double](d, d) * 0.4 - (DenseMatrix.ones[Double](d, d) * 0.20)}

    val inSampleData = Surrogate.generateVAR(
      actualParams,
      d,
      nSamples.toInt,
      deltaT,
      Gaussian(0.0, 0.5),
      DenseVector.ones[Double](d),
      sc)

    println(nSamples + " samples")
    println(d + " dimensions")
    println()

    val (timeSeries, _) = SingleAxisVectTS(nPartitions, config, inSampleData)

    val (estimParams, sigma) = VARModel(timeSeries, p)

    for(lagIdx <- 0 until p){

      val estimMatrix = estimParams(lagIdx)
      val actualMatrix = actualParams(lagIdx)

      for(i <- 0 until d){
        for(j <- 0 until d){

          estimMatrix(i, j) should be (actualMatrix(i, j) +- 0.01)

        }
      }

    }

    for(i <- 0 until d) {
      for (j <- 0 until d) {

        if(i == j) {
          sigma(i, j) should be(0.25 +- 0.8)
        }else{
          sigma(i, j) should be(0.0 +- 0.8)
        }

      }
    }

  }


}
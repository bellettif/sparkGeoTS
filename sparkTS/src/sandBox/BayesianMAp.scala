package sandBox

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.numerics.abs
import breeze.stats.distributions.Gaussian
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import overlapping.containers._
import overlapping.timeSeries._

object BayesianMAp {

  implicit def signedDistMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

  implicit def signedDistLong = (t1: Long, t2: Long) => (t2 - t1).toDouble

  def main(args: Array[String]): Unit = {

    val d = 3
    val N = 10000L
    val paddingMillis = 100L
    val deltaTMillis = 1L
    val nPartitions = 8

    implicit val config = TSConfig(deltaTMillis, d, N, paddingMillis.toDouble)

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)

    val MACoeffs = Array(
      DenseMatrix((0.30, 0.0, 0.0), (0.0, -0.20, 0.0), (0.0, 0.0, -0.45)),
      DenseMatrix((0.12, 0.0, 0.0), (0.0, 0.08, 0.0), (0.0, 0.0, 0.45)),
      DenseMatrix((-0.08, 0.0, 0.0), (0.0, 0.05, 0.0), (0.0, 0.0, 0.00))
    )

    val p = MACoeffs.length

    println(Stability(MACoeffs))

    val noiseMagnitudes = DenseVector.ones[Double](d)

    val rawTS = Surrogate.generateVMA(
      MACoeffs,
      d,
      N.toInt,
      deltaTMillis,
      Gaussian(0.0, 1.0),
      noiseMagnitudes,
      sc)

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    /*
    ##################################

    Multivariate analysis

    ##################################
     */

    val (freqVMAMatrices, _) = VMAModel(overlappingRDD, p)

    freqVMAMatrices.foreach({x => println(x); println()})

    println("Frequentist L1 estimation error")
    println(sum(abs(freqVMAMatrices(0) - MACoeffs(0))))
    println()

    val denseVMAMatrices = VMAGradientDescent(overlappingRDD, p)

    denseVMAMatrices.foreach({x => println(x); println()})

    println("Bayesian L1 estimation error")
    println(sum(abs(denseVMAMatrices(0) - MACoeffs(0))))
    println()


    /*
    ################################

    Sparse Bayesian analysis

    ################################
     */

    /*
    val sparseVARMatrices = VARL1GradientDescent(overlappingRDD, p, 1e-2)

    println("Sparse Bayesian L1 estimation error")
    println(sum(abs(sparseVARMatrices(0) - ARCoeffs(0))))
    println()

    val residualsSparseVAR = VARPredictor(overlappingRDD, sparseVARMatrices, Some(mean))
    val residualSecondMomentSparseVAR = SecondMomentEstimator(residualsSparseVAR)

    println("Sparse VAR residuals")
    println(trace(residualSecondMomentSparseVAR))
    println()
    */

  }
}
package showcase

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.numerics.abs
import breeze.plot.{Figure, image}
import breeze.stats.distributions.Gaussian
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import overlapping.containers.block.SingleAxisBlock
import overlapping.io.SingleAxisBlockRDD
import overlapping.models.firstOrder.{MeanEstimator, SecondMomentEstimator}
import overlapping.models.secondOrder.multivariate.VARPredictor
import overlapping.models.secondOrder.multivariate.bayesianEstimators.{VARL1GradientDescent, AutoregressiveGradient, AutoregressiveLoss, VARGradientDescent}
import overlapping.models.secondOrder.multivariate.bayesianEstimators.gradients.DiagonalNoiseARGrad
import overlapping.models.secondOrder.multivariate.bayesianEstimators.lossFunctions.DiagonalNoiseARLoss
import overlapping.models.secondOrder.multivariate.frequentistEstimators.{CrossCovariance, VARModel}
import overlapping.models.secondOrder.univariate.{ARModel, ARPredictor, AutoCovariances}
import overlapping.surrogateData.{IndividualRecords, TSInstant}

import scala.math.Ordering

object SurrogateAR1 {

  def main(args: Array[String]): Unit = {

    val filePath = "/users/cusgadmin/traffic_data/uber-ny/uber_spatial_bins_20x20_merged.csv"

    val d             = 100
    val b             = 3
    val N             = 200000L
    val paddingMillis = 100L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val A = DenseMatrix.rand[Double](d, d)

    for(i <- 0 until d){
      for(j <- 0 until d){
        if(abs(i - j) > b){
          A(i, j) = 0.0
        }
      }
    }

    /*
    for(i <- 0 until d){
      for(j <- 0 until d){
        if((j == 0) && (i == d - 1)){

        }else{
          if(j != i + 1){
            A(i, j) = 0.0
          }
        }
      }
    }
    */

    val svd.SVD(_, sA, _) = svd(A)

    A :*= 1.0 / (max(sA) * 1.1)

    val ARcoeffs = Array(A)
    val noiseMagnitudes = DenseVector.rand[Double](d)

    val rawTS = IndividualRecords.generateVAR(
      ARcoeffs,
      d,
      N.toInt,
      deltaTMillis,
      Gaussian(0.0, 1.0),
      noiseMagnitudes,
      sc)

    val f0 = Figure()
    f0.subplot(0) += image(ARcoeffs(0))
    f0.saveas("AR_coeffs.png")

    val f1 = Figure()
    f1.subplot(0) += image(diag(noiseMagnitudes))
    f1.saveas("noise_magnitudes.png")

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    /*
     Estimate process' mean
     */
    val meanEstimator = new MeanEstimator[TSInstant]()
    val secondMomentEstimator = new SecondMomentEstimator[TSInstant]()

    val mean = meanEstimator.estimate(overlappingRDD)

    /*
    ################################

    Monovariate analysis

    ################################
     */
    val p = 1

    val autoCovEstimator = new AutoCovariances[TSInstant](deltaTMillis, p, d, sc.broadcast(mean))
    val autocovariances = autoCovEstimator.estimate(overlappingRDD)


    val freqAREstimator = new ARModel[TSInstant](deltaTMillis, p, d, sc.broadcast(mean))
    val vectorsAR = freqAREstimator.estimate(overlappingRDD)

    val predictorAR = new ARPredictor[TSInstant](
      deltaTMillis,
      p,
      d,
      sc.broadcast(mean),
      sc.broadcast(vectorsAR.map(x => x.covariation)))

    val predictionsAR = predictorAR.predictAll(overlappingRDD)
    val residualsAR = predictorAR.residualAll(overlappingRDD)
    val residualMeanAR = meanEstimator.estimate(residualsAR)
    val residualSecondMomentAR = secondMomentEstimator.estimate(residualsAR)

    println("AR error")
    println(trace(residualSecondMomentAR))
    println()

    val f2 = Figure()
    f2.subplot(0) += image(residualSecondMomentAR)
    f2.saveas("cov_freq_AR_residuals.png")

    /*
    ##################################

    Multivariate analysis

    ##################################
     */

    val crossCovarianceEstimator = new CrossCovariance[TSInstant](
      deltaTMillis,
      p,
      d,
      sc.broadcast(mean))
    val (crossCovariances, covMatrix) = crossCovarianceEstimator.estimate(overlappingRDD)

    val freqVAREstimator = new VARModel[TSInstant](
      deltaTMillis,
      p,
      d,
      sc.broadcast(mean))

    val (freqVARmatrices, _) = freqVAREstimator.estimate(overlappingRDD)

    val f3 = Figure()
    f3.subplot(0) += image(freqVARmatrices(0))
    f3.saveas("freq_VAR_coeffs.png")

    val predictorVAR = new VARPredictor[TSInstant](
      deltaTMillis,
      p,
      d,
      sc.broadcast(mean),
      sc.broadcast(freqVARmatrices))

    val predictionsVAR = predictorVAR.predictAll(overlappingRDD)
    val residualsVAR = predictorVAR.residualAll(overlappingRDD)
    val residualMeanVAR = meanEstimator.estimate(residualsVAR)
    val residualSecondMomentVAR = secondMomentEstimator.estimate(residualsVAR)

    println("VAR residuals")
    println(trace(residualSecondMomentVAR))
    println()

    val f4 = Figure()
    f4.subplot(0) += image(residualSecondMomentAR)
    f4.saveas("cov_freq_VAR_residuals.png")

    /*
    ##################################

    Bayesian multivariate analysis

    ##################################
     */

    val svd.SVD(_, s, _) = svd(covMatrix)

    val sigmaEpsilon = diag(residualSecondMomentVAR)

    def stepSize(x: Int): Double ={
      1.0 / (max(s) * max(sigmaEpsilon)
        + min(s) * min(sigmaEpsilon))
    }

    val VARLoss = new DiagonalNoiseARLoss(sigmaEpsilon, N, sc.broadcast(mean))
    val VARGrad = new DiagonalNoiseARGrad(sigmaEpsilon, N, sc.broadcast(mean))

    val VARBayesEstimator = new VARGradientDescent[TSInstant](
      p,
      deltaTMillis,
      new AutoregressiveLoss(
        p,
        deltaTMillis,
        Array.fill(p){DenseMatrix.zeros[Double](d, d)},
        {case (param, data) => VARLoss(param, data)}),
      new AutoregressiveGradient(
        p,
        deltaTMillis,
        Array.fill(p){DenseMatrix.zeros[Double](d, d)},
        {case (param, data) => VARGrad(param, data)}),
      stepSize,
      1e-5,
      100,
      freqVARmatrices
    )

    val denseVARMatrices = VARBayesEstimator.estimate(overlappingRDD)

    val f5= Figure()
    f5.subplot(0) += image(denseVARMatrices(0))
    f5.saveas("bayesian_VAR_coeffs.png")

    val predictorBayesianVAR = new VARPredictor[TSInstant](
      deltaTMillis,
      p,
      d,
      sc.broadcast(mean),
      sc.broadcast(denseVARMatrices))

    val predictionsBayesianVAR = predictorBayesianVAR.predictAll(overlappingRDD)
    val residualsBayesianVAR = predictorBayesianVAR.residualAll(overlappingRDD)
    val residualMeanBayesianVAR = meanEstimator.estimate(residualsBayesianVAR)
    val residualSecondMomentBayesianVAR = secondMomentEstimator.estimate(residualsBayesianVAR)

    println("Bayesian VAR residuals")
    println(trace(residualSecondMomentBayesianVAR))
    println()

    val f6= Figure()
    f6.subplot(0) += image(residualSecondMomentBayesianVAR)
    f6.saveas("cov_bayesian_VAR_residuals.png")

    /*
    ################################

    Sparse Bayesian analysis

    ################################
     */

    val VARSparseEstimator = new VARL1GradientDescent[TSInstant](
      p,
      deltaTMillis,
      new AutoregressiveLoss(
      p,
      deltaTMillis,
      Array.fill(p){DenseMatrix.zeros[Double](d, d)},
      {case (param, data) => VARLoss(param, data)}),
      new AutoregressiveGradient(
      p,
      deltaTMillis,
      Array.fill(p){DenseMatrix.zeros[Double](d, d)},
      {case (param, data) => VARGrad(param, data)}),
      stepSize,
      1e-5,
      1e-3,
      100,
      freqVARmatrices
    )

    val sparseVARMatrices = VARSparseEstimator.estimate(overlappingRDD)

    val f7= Figure()
    f7.subplot(0) += image(sparseVARMatrices(0))
    f7.saveas("sparse_VAR_coeffs.png")

    val predictorSparseVAR = new VARPredictor[TSInstant](
      deltaTMillis,
      p,
      d,
      sc.broadcast(mean),
      sc.broadcast(sparseVARMatrices))

    val predictionsSparseVAR = predictorSparseVAR.predictAll(overlappingRDD)
    val residualsSparseVAR = predictorSparseVAR.residualAll(overlappingRDD)
    val residualMeanSparseVAR = meanEstimator.estimate(residualsSparseVAR)
    val residualSecondMomentSparseVAR = secondMomentEstimator.estimate(residualsSparseVAR)

    println("Sparse VAR residuals")
    println(trace(residualSecondMomentSparseVAR))
    println()

    val f8= Figure()
    f8.subplot(0) += image(residualSecondMomentSparseVAR)
    f8.saveas("cov_sparse_VAR_residuals.png")


  }
}
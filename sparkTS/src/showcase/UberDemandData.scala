package showcase

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.plot.{Figure, image}
import ioTools.ReadCsv
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import overlapping.containers.block.SingleAxisBlock
import overlapping.io.SingleAxisBlockRDD
import overlapping.models.firstOrder.procedures.RbfSmoothing
import overlapping.models.firstOrder.{MeanProfileEstimator, MeanEstimator, SecondMomentEstimator}
import overlapping.models.secondOrder.multivariate.VARPredictor
import overlapping.models.secondOrder.multivariate.bayesianEstimators.gradients.DiagonalNoiseARGrad
import overlapping.models.secondOrder.multivariate.bayesianEstimators.lossFunctions.DiagonalNoiseARLoss
import overlapping.models.secondOrder.multivariate.bayesianEstimators.{AutoregressiveGradient, AutoregressiveLoss, VARGradientDescent, VARL1GradientDescent}
import overlapping.models.secondOrder.multivariate.frequentistEstimators.VARModel
import overlapping.models.secondOrder.univariate.{ARModel, ARPredictor}
import overlapping.surrogateData.TSInstant

import scala.math.Ordering

object UberDemandData {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /*
    ##########################################

      In sample analysis

    ##########################################
     */

    val inSampleFilePath = "/users/cusgadmin/traffic_data/uber-ny/uber_spatial_bins_20x20_merged.csv"
    val inSampleData = ReadCsv(inSampleFilePath, 0, "yyyy-MM-dd HH:mm:ss", true)

    val d             = inSampleData.head._2.length
    val nSamples      = inSampleData.length
    val paddingMillis = 6000000L // 100 minutes
    val deltaTMillis  = 60000L // 1 minute
    val nPartitions   = 8

    val inSampleData_ = sc.parallelize(inSampleData)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (rawTimeSeries: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, inSampleData_)

    /*
    ############################################

      Get rid of seasonality

    ############################################
     */

    /*
    def hashFunction(x: TSInstant): Int = {
      (x.timestamp.getDayOfWeek - 1) * 24 * 60 + (x.timestamp.getMinuteOfDay - 1)
    }

    val meanProfileEstimator = new MeanProfileEstimator[TSInstant](
      d,
      hashFunction)

    val meanProfile = meanProfileEstimator.estimate(rawTimeSeries)

    val matrixMeanProfile = DenseMatrix.zeros[Double](7 * 24 * 60, d)

    for(k <- meanProfile.keys){
      matrixMeanProfile(k, ::) := meanProfile(k).t
    }

    val f0 = Figure()
    f0.subplot(0) += image(matrixMeanProfile)
    f0.saveas("weekly_profile.png")

    val seasonalProfile = sc.broadcast(meanProfile)

    val rawInSampleNoSeason = inSampleData_
    //val rawInSampleNoSeason = inSampleData_.map({case (k, v) => (k, v - seasonalProfile.value(hashFunction(k)))})
    */

    /*
     ##################################################

       Estimate process mean

     ##################################################
     */

    val (inSampleTimeSeries: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, inSampleData_)

    val meanEstimator = new MeanEstimator[TSInstant](d)
    val secondMomentEstimator = new SecondMomentEstimator[TSInstant](d)

    val mean = meanEstimator.estimate(inSampleTimeSeries)

    /*
    ###################################################

      Monovariate analysis

    ###################################################
     */
    val p = 1
    val freqAREstimator = new ARModel[TSInstant](deltaTMillis, p, d, sc.broadcast(mean))
    val vectorsAR = freqAREstimator.estimate(inSampleTimeSeries)

    val predictorAR = new ARPredictor[TSInstant](
      deltaTMillis,
      p,
      d,
      sc.broadcast(mean),
      sc.broadcast(vectorsAR.map(x => x.covariation)))

    val predictionsAR = predictorAR.predictAll(inSampleTimeSeries)
    val residualsAR = predictorAR.residualAll(inSampleTimeSeries)
    val residualMeanAR = meanEstimator.estimate(residualsAR)
    val residualSecondMomentAR = secondMomentEstimator.estimate(residualsAR)

    println("AR frequentist analysis:")
    println(max(vectorsAR.map(x => max(x.covariation))))
    println(min(vectorsAR.map(x => min(x.covariation))))
    println(residualMeanAR)
    println(trace(residualSecondMomentAR))
    println()

    val f1 = Figure()
    f1.subplot(0) += image(residualSecondMomentAR)
    f1.saveas("residuals_AR.png")

    /*
    #################################################

    Multivariate analysis

    #################################################
     */
    val freqVAREstimator = new VARModel[TSInstant](deltaTMillis, 1, d, sc.broadcast(mean))
    val (freqVARMatrices, covMatrix) = freqVAREstimator.estimate(inSampleTimeSeries)

    val svd.SVD(_, sVAR, _) = svd(freqVARMatrices(0))

    val predictorVAR = new VARPredictor[TSInstant](
      deltaTMillis,
      p,
      d,
      sc.broadcast(mean),
      sc.broadcast(freqVARMatrices))

    val predictionsVAR = predictorVAR.predictAll(inSampleTimeSeries)
    val residualsVAR = predictorVAR.residualAll(inSampleTimeSeries)
    val residualMeanVAR = meanEstimator.estimate(residualsVAR)
    val residualSecondMomentVAR = secondMomentEstimator.estimate(residualsVAR)

    println("VAR frequentist analysis")
    println(max(sVAR))
    println(min(sVAR))
    println(residualMeanVAR)
    println(trace(residualSecondMomentVAR))
    println()

    val f2 = Figure()
    f2.subplot(0) += image(residualSecondMomentVAR)
    f2.saveas("residuals_VAR.png")

    val f3 = Figure()
    f3.subplot(0) += image(freqVARMatrices(0))
    f3.saveas("coeffs_VAR.png")

    /*
    /*
    Multivariate Bayesian analysis
     */
    val VARLoss = new DiagonalNoiseARLoss(diag(residualSecondMomentVAR), nSamples, sc.broadcast(mean))
    val VARGrad = new DiagonalNoiseARGrad(diag(residualSecondMomentVAR), nSamples, sc.broadcast(mean))

    val svd.SVD(_, s, _) = svd(covMatrix)

    def stepSize(x: Int): Double ={
      1.0 / (max(s) * max(diag(residualSecondMomentVAR)) + min(s) * min(diag(residualSecondMomentVAR)))
    }

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
      1000,
      freqVARmatrices
    )

    val bayesianVAR = VARBayesEstimator.estimate(inSampleTimeSeries)

    val f4 = Figure()
    f4.subplot(0) += image(bayesianVAR(0))
    f4.saveas("coeffs_bayesian_VAR.png")

    val sparseVARBayesEstimator = new VARL1GradientDescent[TSInstant](
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
      1e-2,
      1000,
      freqVARmatrices
    )

    val sparseBayesianVAR = sparseVARBayesEstimator.estimate(inSampleTimeSeries)

    val f5 = Figure()
    f5.subplot(0) += image(sparseBayesianVAR(0))
    f5.saveas("coeffs_sparse_bayesian_VAR.png")

    */

  }
}
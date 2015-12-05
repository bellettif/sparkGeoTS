package main.scala.overlapping.timeSeries

import breeze.linalg._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import main.scala.overlapping.containers._
import main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.gradients.DiagonalNoiseARGrad
import main.scala.overlapping.timeSeries.secondOrder.multivariate.lossFunctions.DiagonalNoiseARLoss
import main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.procedures.{GradientDescent, L1ClippedGradientDescent}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/16/15.
 */
object VARL1GradientDescent{

  def apply[IndexT <: TSInstant[IndexT] : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      p: Int,
      lambda: Double,
      precision: Double = 1e-6,
      maxIter: Int = 1000): Array[DenseMatrix[Double]] = {

    val estimator = new VARL1GradientDescent[IndexT](p, lambda, timeSeries.config, precision, maxIter)
    estimator.estimate(timeSeries)

  }

}


class VARL1GradientDescent[IndexT <: TSInstant[IndexT] : ClassTag](
    p: Int,
    lambda: Double,
    config: VectTSConfig[IndexT],
    precision: Double = 1e-6,
    maxIter: Int = 1000)
  extends Estimator[IndexT, DenseVector[Double], Array[DenseMatrix[Double]]]{

  val d = config.dim
  val N = config.nSamples
  val deltaT = config.deltaT

  if(deltaT * p > config.bckPadding){
    throw new IndexOutOfBoundsException("Not enough padding to support model estimation.")
  }

  override def estimate(timeSeries: TimeSeries[IndexT, DenseVector[Double]]): Array[DenseMatrix[Double]] = {

    val mean = new MeanEstimator(config).estimate(timeSeries)

    val freqVAREstimator = new VARModel[IndexT](p, config, timeSeries.content.context.broadcast(Some(mean)))
    val (freqVARMatrices, _) = freqVAREstimator.estimate(timeSeries)

    val predictorVAR = new VARPredictor[IndexT](
      timeSeries.content.context.broadcast(freqVARMatrices),
      timeSeries.content.context.broadcast(Some(mean)))
    val residualsVAR = predictorVAR.estimateResiduals(timeSeries)

    val residualSecondMomentVAR = new SecondMomentEstimator(config).estimate(residualsVAR)
    val sigmaEpsilon = diag(residualSecondMomentVAR)

    val covEstimator = new Covariance[IndexT](config, timeSeries.content.context.broadcast(Some(mean)))
    val covMatrix = covEstimator.estimate(timeSeries)
    val svd.SVD(_, s, _) = svd(covMatrix)
    def stepSize(x: Int): Double ={
      1.0 / (max(s) * max(sigmaEpsilon) + min(s) * min(sigmaEpsilon))
    }

    val VARLoss = new DiagonalNoiseARLoss[IndexT](sigmaEpsilon, N, timeSeries.content.context.broadcast(mean))
    val VARGrad = new DiagonalNoiseARGrad[IndexT](sigmaEpsilon, N, timeSeries.content.context.broadcast(mean))

    val kernelizedLoss = new AutoregressiveLoss[IndexT](p, VARLoss.apply, config)
    val kernelizedGrad = new AutoregressiveGradient[IndexT](p, VARGrad.apply, config)

    val gradSizes = kernelizedGrad.getGradientSize

    L1ClippedGradientDescent.run[TimeSeries[IndexT, DenseVector[Double]]](
      {case (param, data) => kernelizedLoss.setNewX(param); kernelizedLoss.timeSeriesStats(data)},
      {case (param, data) => kernelizedGrad.setNewX(param); kernelizedGrad.timeSeriesStats(data)},
      gradSizes,
      stepSize,
      precision,
      lambda,
      maxIter,
      freqVARMatrices,
      timeSeries
    )

  }

}

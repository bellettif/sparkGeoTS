package main.scala.overlapping.timeSeriesOld

import breeze.linalg._
import main.scala.overlapping.timeSeries._
import main.scala.overlapping.timeSeriesOld.secondOrder.multivariate.bayesianEstimators.gradients.DiagonalNoiseARGrad
import main.scala.overlapping.timeSeriesOld.secondOrder.multivariate.bayesianEstimators.procedures.L1ClippedGradientDescent
import main.scala.overlapping.timeSeriesOld.secondOrder.multivariate.lossFunctions.DiagonalNoiseARLoss

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/16/15.
 */
object VARL1GradientDescent{

  def apply[IndexT : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      p: Int,
      lambda: Double,
      precision: Double = 1e-6,
      maxIter: Int = 1000): Array[DenseMatrix[Double]] = {

    val estimator = new VARL1GradientDescent[IndexT](p, lambda, timeSeries.config, precision, maxIter)
    estimator.estimate(timeSeries)

  }

}


class VARL1GradientDescent[IndexT : ClassTag](
    p: Int,
    lambda: Double,
    config: VectTSConfig[IndexT],
    precision: Double = 1e-6,
    maxIter: Int = 1000)
  extends Estimator[IndexT, Array[DenseMatrix[Double]]]{

  val d = config.dim
  val N = config.nSamples
  val deltaT = config.deltaT

  if(deltaT * p > config.bckPadding){
    throw new IndexOutOfBoundsException("Not enough padding to support model estimation.")
  }

  override def estimate(timeSeries: VectTimeSeries[IndexT]): Array[DenseMatrix[Double]] = {

    val mean = new MeanEstimator(config).estimate(timeSeries)

    val freqVAREstimator = new VARModel[IndexT](p, config, timeSeries.content.context.broadcast(Some(mean)))
    val (freqVARMatrices, _) = freqVAREstimator.estimate(timeSeries)

    val residualsVAR = VARPredictor(timeSeries, freqVARMatrices, Some(mean))

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

    L1ClippedGradientDescent.run[VectTimeSeries[IndexT]](
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

package main.scala.overlapping.timeSeries

import breeze.linalg._
import breeze.numerics.abs
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import main.scala.overlapping.containers._
import main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.gradients.DiagonalNoiseARGrad
import main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.procedures.GradientDescent
import main.scala.overlapping.timeSeries.secondOrder.multivariate.lossFunctions.DiagonalNoiseARLoss

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/16/15.
 */

object VARGradientDescent{

  def apply[IndexT <: TSInstant[IndexT] : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      p: Int,
      precision: Double = 1e-4,
      maxIter: Int = 100): Array[DenseMatrix[Double]] = {

      val estimator = new VARGradientDescent[IndexT](p, timeSeries.config, precision, maxIter)
      estimator.estimate(timeSeries)

  }

}


class VARGradientDescent[IndexT <: TSInstant[IndexT] : ClassTag](
    p: Int,
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

    val (freqVARMatrices, noiseVariance) = new VARModel(
      p,
      config,
      timeSeries.content.context.broadcast(Some(mean))).estimate(timeSeries)

    val sigmaEpsilon = diag(noiseVariance)

    /*
    Redundant computation of cross-cov Matrix, need to do something about that
     */
    val (crossCovMatrices, _) = new CrossCovariance(
      p,
      config,
      timeSeries.content.context.broadcast(Some(mean))).estimate(timeSeries)

    val allEigenValues = crossCovMatrices.map(x => abs(eig(x).eigenvalues))
    val maxEig = max(allEigenValues.map(x => max(x)))
    val minEig = min(allEigenValues.map(x => min(x)))

    def stepSize(x: Int): Double ={
      2.0 / (maxEig * max(sigmaEpsilon) + minEig * min(sigmaEpsilon))
    }

    val VARLoss = new DiagonalNoiseARLoss[IndexT](sigmaEpsilon, N, timeSeries.content.context.broadcast(mean))
    val VARGrad = new DiagonalNoiseARGrad[IndexT](sigmaEpsilon, N, timeSeries.content.context.broadcast(mean))

    val kernelizedLoss = new AutoregressiveLoss[IndexT](p, VARLoss.apply, config)
    val kernelizedGrad = new AutoregressiveGradient[IndexT](p, VARGrad.apply, config)

    val gradSizes = kernelizedGrad.getGradientSize

    GradientDescent.run[TimeSeries[IndexT, DenseVector[Double]]](
      {case (param, data) => kernelizedLoss.setNewX(param); kernelizedLoss.timeSeriesStats(data)},
      {case (param, data) => kernelizedGrad.setNewX(param); kernelizedGrad.timeSeriesStats(data)},
      gradSizes,
      stepSize,
      precision,
      maxIter,
      freqVARMatrices,
      timeSeries
    )

  }

}

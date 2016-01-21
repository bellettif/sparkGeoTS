package main.scala.overlapping.timeSeriesOld

import breeze.linalg._
import breeze.numerics.abs

import scala.reflect.ClassTag


object VMAGradientDescent{

  def apply[IndexT : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      p: Int,
      precision: Double = 1e-4,
      maxIter: Int = 100): Array[DenseMatrix[Double]] = {

    val estimator = new VMAGradientDescent[IndexT](p, timeSeries.config, precision, maxIter)
    estimator.estimate(timeSeries)

  }

}

class VMAGradientDescent[IndexT : ClassTag](
    q: Int,
    config: VectTSConfig[IndexT],
    precision: Double = 1e-4,
    maxIter: Int = 1000)
  extends Estimator[IndexT, Array[DenseMatrix[Double]]]{

  val d = config.dim
  val N = config.nSamples
  val deltaT = config.deltaT

  if(deltaT * q > config.bckPadding){
    throw new IndexOutOfBoundsException("Not enough padding to support model estimation.")
  }

  override def estimate(timeSeries: VectTimeSeries[IndexT]): Array[DenseMatrix[Double]] = {

    val mean = new MeanEstimator(config).estimate(timeSeries)

    val (freqVMAMatrices, noiseVariance) = new VMAModel(q, config, timeSeries.content.context.broadcast(Some(mean))).estimate(timeSeries)

    val sigmaEpsilon = diag(noiseVariance)

    /*
    Redundant computation of cross-cov Matrix, need to do something about that
     */
    val (crossCovMatrices, _) = new CrossCovariance(q, config, timeSeries.content.context.broadcast(Some(mean))).estimate(timeSeries)

    val allEigenValues = crossCovMatrices.map(x => abs(eig(x).eigenvalues))
    val maxEig = max(allEigenValues.map(x => max(x)))
    val minEig = min(allEigenValues.map(x => min(x)))

    def stepSize(x: Int): Double ={
      0.9 / (maxEig * max(sigmaEpsilon) + minEig * min(sigmaEpsilon))
    }

    val VMALoss = new DiagonalNoiseMALoss[IndexT](sigmaEpsilon, N, timeSeries.content.context.broadcast(mean))
    val VMAGrad = new DiagonalNoiseMAGrad[IndexT](sigmaEpsilon, N, timeSeries.content.context.broadcast(mean))

    val kernelizedLoss = new MemoryLoss(q, VMALoss.apply, config)
    val kernelizedGrad = new MemoryGradient(q, VMAGrad.apply, config)

    val gradSizes = kernelizedGrad.getGradientSize

    GradientDescent.run[VectTimeSeries[IndexT]](
      {case (param, data) => kernelizedLoss.setNewX(param); kernelizedLoss.timeSeriesStats(data)},
      {case (param, data) => kernelizedGrad.setNewX(param); kernelizedGrad.timeSeriesStats(data)},
      gradSizes,
      stepSize,
      precision,
      maxIter,
      freqVMAMatrices,
      timeSeries
    )

  }

}

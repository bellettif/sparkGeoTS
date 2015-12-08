package main.scala.overlapping.timeSeriesOld

import breeze.linalg._
import main.scala.overlapping.timeSeries.secondOrder.multivariate.frequentistEstimators.procedures.ToeplitzMulti
import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag


object PartialCrossCorrelation{

  def apply[IndexT : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      maxLag: Int,
      mean: Option[DenseVector[Double]] = None): (Array[DenseMatrix[Double]], DenseMatrix[Double]) ={

    val estimator = new PartialCrossCorrelation[IndexT](
      maxLag,
      timeSeries.config,
      timeSeries.content.context.broadcast(mean))

    estimator.estimate(timeSeries)

  }

}


/**
Here we expect the number of dimensions to be the same for all records.

The autocovoriance is ordered as follows

-modelOrder ... 0 ... modelOrder
 */

class PartialCrossCorrelation[IndexT : ClassTag](
    maxLag: Int,
    config: VectTSConfig[IndexT],
    mean: Broadcast[Option[DenseVector[Double]]])
  extends CrossCovariance[IndexT](maxLag, config, mean){

  def estimatePrecisionMatrices(crossCovMatrices: Array[DenseMatrix[Double]]): Array[DenseMatrix[Double]] ={
    val nCols = crossCovMatrices.head.rows

    val coeffMatrices = ToeplitzMulti(maxLag, nCols,
      crossCovMatrices.slice(1, 2 * maxLag),
      crossCovMatrices.slice(maxLag+ 1, 2 * maxLag + 1))

    coeffMatrices.foreach(x => x := x.t)

    coeffMatrices
  }

  override def estimate(timeSeries: VectTimeSeries[IndexT]):
    (Array[DenseMatrix[Double]], DenseMatrix[Double])={

    val covarianceMatrices = normalize(
      timeSeriesStats(timeSeries)
    )

    val partialAutoCovs = estimatePrecisionMatrices(covarianceMatrices)

    (partialAutoCovs, partialAutoCovs(0))

  }


}
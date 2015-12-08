package main.scala.overlapping.timeSeriesOld

import breeze.linalg.{DenseMatrix, DenseVector}
import main.scala.overlapping.timeSeriesOld.CrossCovariance
import main.scala.overlapping.timeSeriesOld.secondOrder.multivariate.frequentistEstimators.procedures.InnovationAlgoMulti
import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/13/15.
 */

object VMAModel{

  def apply[IndexT : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      q: Int,
      mean: Option[DenseVector[Double]] = None): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val estimator = new VMAModel[IndexT](q, timeSeries.config, timeSeries.content.context.broadcast(mean))
    estimator.estimate(timeSeries)

  }

}

class VMAModel[IndexT : ClassTag](
    q: Int,
    config: VectTSConfig[IndexT],
    mean: Broadcast[Option[DenseVector[Double]]])
  extends CrossCovariance[IndexT](q, config, mean){

  def estimateVMAMatrices(crossCovMatrices: Array[DenseMatrix[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) ={

    InnovationAlgoMulti(q, crossCovMatrices)

  }

  override def estimate(timeSeries: VectTimeSeries[IndexT]): (Array[DenseMatrix[Double]], DenseMatrix[Double])= {

    val crossCovMatrices: Array[DenseMatrix[Double]] = super.estimate(timeSeries)._1
    estimateVMAMatrices(crossCovMatrices)

  }

}
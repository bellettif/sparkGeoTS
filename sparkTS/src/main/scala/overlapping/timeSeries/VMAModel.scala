package main.scala.overlapping.timeSeries

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import main.scala.overlapping.containers._
import main.scala.overlapping.timeSeries.secondOrder.multivariate.frequentistEstimators.procedures.InnovationAlgoMulti

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/13/15.
 */

object VMAModel{

  def apply[IndexT <: TSInstant[IndexT] : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      q: Int,
      mean: Option[DenseVector[Double]] = None)
      (implicit config: TSConfig): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val estimator = new VMAModel[IndexT](q, timeSeries.config, timeSeries.content.context.broadcast(mean))
    estimator.estimate(timeSeries)

  }

}

class VMAModel[IndexT <: TSInstant[IndexT] : ClassTag](
    q: Int,
    config: VectTSConfig[IndexT],
    mean: Broadcast[Option[DenseVector[Double]]])
  extends CrossCovariance[IndexT](q, config, mean){

  def estimateVMAMatrices(crossCovMatrices: Array[DenseMatrix[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) ={

    InnovationAlgoMulti(q, crossCovMatrices)

  }

  override def estimate(timeSeries: TimeSeries[IndexT, DenseVector[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double])= {

    val crossCovMatrices: Array[DenseMatrix[Double]] = super.estimate(timeSeries)._1
    estimateVMAMatrices(crossCovMatrices)

  }

}
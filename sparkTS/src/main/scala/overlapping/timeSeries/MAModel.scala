package main.scala.overlapping.timeSeries

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import main.scala.overlapping.containers._
import main.scala.overlapping.timeSeries.secondOrder.univariate.Procedures.InnovationAlgo

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/13/15.
 */

object MAModel{

  def apply[IndexT <: TSInstant[IndexT] : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      q: Int,
      mean: Option[DenseVector[Double]] = None)
      (implicit config: TSConfig): Array[SecondOrderSignature] ={

    val estimator = new MAModel[IndexT](
      q,
      timeSeries.config,
      timeSeries.content.context.broadcast(mean))
    estimator.estimate(timeSeries)

  }

}

class MAModel[IndexT <: TSInstant[IndexT] : ClassTag](
    q: Int,
    config: VectTSConfig[IndexT],
    mean: Broadcast[Option[DenseVector[Double]]])
  extends AutoCovariances[IndexT](q, config, mean){

  override def estimate(timeSeries: TimeSeries[IndexT, DenseVector[Double]]): Array[SecondOrderSignature]= {

    super
      .estimate(timeSeries)
      .map(x => InnovationAlgo(q, x.covariation))

  }


}
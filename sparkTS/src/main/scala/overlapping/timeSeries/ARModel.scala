package main.scala.overlapping.timeSeries

import breeze.linalg.DenseVector
import org.apache.spark.broadcast.Broadcast
import main.scala.overlapping.containers._
import main.scala.overlapping.timeSeries.secondOrder.univariate.Procedures.DurbinLevinson

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/13/15.
 */
object ARModel{

  def apply[IndexT <: TSInstant[IndexT] : ClassTag](
      timeSeries : VectTimeSeries[IndexT],
      p: Int,
      mean: Option[DenseVector[Double]] = None): Array[SecondOrderSignature] ={

    val estimator = new ARModel[IndexT](
      p,
      timeSeries.config,
      timeSeries.content.context.broadcast(mean))

    estimator.estimate(timeSeries)

  }

}

class ARModel[IndexT <: TSInstant[IndexT] : ClassTag](
    p: Int,
    config: VectTSConfig[IndexT],
    mean: Broadcast[Option[DenseVector[Double]]])
  extends AutoCovariances[IndexT](p, config, mean){

  override def estimate(timeSeries: TimeSeries[IndexT, DenseVector[Double]]): Array[SecondOrderSignature]= {

    super
      .estimate(timeSeries)
      .map(x => DurbinLevinson(p, x.covariation))

  }



}
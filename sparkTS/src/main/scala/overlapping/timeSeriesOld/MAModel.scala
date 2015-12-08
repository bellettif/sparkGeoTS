package main.scala.overlapping.timeSeriesOld

import breeze.linalg.DenseVector
import main.scala.overlapping.timeSeries.secondOrder.univariate.Procedures.InnovationAlgo
import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/13/15.
 */

object MAModel{

  def apply[IndexT : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      q: Int,
      mean: Option[DenseVector[Double]] = None): Array[SecondOrderSignature] ={

    val estimator = new MAModel[IndexT](
      q,
      timeSeries.config,
      timeSeries.content.context.broadcast(mean))
    estimator.estimate(timeSeries)

  }

}

class MAModel[IndexT : ClassTag](
    q: Int,
    config: VectTSConfig[IndexT],
    mean: Broadcast[Option[DenseVector[Double]]])
  extends AutoCovariances[IndexT](q, config, mean){

  override def estimate(timeSeries: VectTimeSeries[IndexT]): Array[SecondOrderSignature]= {

    super
      .estimate(timeSeries)
      .map(x => InnovationAlgo(q, x.covariation))

  }


}
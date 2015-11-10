package main.scala.overlapping.timeSeries

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import main.scala.overlapping.containers.SingleAxisBlock
import main.scala.overlapping.timeSeries.secondOrder.univariate.Procedures.DurbinLevinson

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/13/15.
 */
object ARModel{

  def apply[IndexT <: Ordered[IndexT] : ClassTag](
      timeSeries : RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
      p: Int,
      mean: Option[DenseVector[Double]] = None)
      (implicit config: TSConfig): Array[SecondOrderSignature] ={

    val estimator = new ARModel[IndexT](p, timeSeries.context.broadcast(mean))
    estimator.estimate(timeSeries)

  }

}

class ARModel[IndexT <: Ordered[IndexT] : ClassTag](
    p: Int,
    mean: Broadcast[Option[DenseVector[Double]]])
    (implicit config: TSConfig)
  extends AutoCovariances[IndexT](p, mean){

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): Array[SecondOrderSignature]= {

    super
      .estimate(timeSeries)
      .map(x => DurbinLevinson(p, x.covariation))

  }



}
package overlapping.models.secondOrder.univariate

import breeze.linalg.DenseVector
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import overlapping.containers.block.SingleAxisBlock
import overlapping.models.Predictor

import overlapping.models.secondOrder.univariate.procedures.DurbinLevinson

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
class ARModel[IndexT <: Ordered[IndexT] : ClassTag](
    deltaT: Double,
    p: Int,
    d: Int,
    mean: Broadcast[DenseVector[Double]]
  )
  extends AutoCovariances[IndexT](deltaT, p, d, mean){

  override def windowEstimate(slice: Array[(IndexT, DenseVector[Double])]): Array[CovSignature] = {

    super
      .windowEstimate(slice)
      .map(x => DurbinLevinson(p, x.covariation))

  }

  override def blockEstimate(block: SingleAxisBlock[IndexT, DenseVector[Double]]): Array[CovSignature] = {

    super
      .blockEstimate(block)
      .map(x => DurbinLevinson(p, x.covariation))

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): Array[CovSignature]= {

    super
      .estimate(timeSeries)
      .map(x => DurbinLevinson(p, x.covariation))

  }



}
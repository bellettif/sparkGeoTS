package overlapping.models.secondOrder.univariate

import org.apache.spark.rdd.RDD
import overlapping.containers.block.SingleAxisBlock

import overlapping.models.secondOrder.univariate.procedures.DurbinLevinson

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
class ARModel[IndexT <: Ordered[IndexT] : ClassTag](deltaT: Double, modelOrder: Int)
  extends AutoCovariances[IndexT](deltaT, modelOrder){

  override def windowEstimate(slice: Array[(IndexT, Array[Double])]): Array[CovSignature] = {

    super
      .estimate(slice)
      .map(x => DurbinLevinson(modelOrder, x.covariation))

  }

  override def blockEstimate(block: SingleAxisBlock[IndexT, Array[Double]]): Array[CovSignature] = {

    super
      .estimate(block)
      .map(x => DurbinLevinson(modelOrder, x.covariation))

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, Array[Double]])]): Array[CovSignature]= {

    super
      .estimate(timeSeries)
      .map(x => DurbinLevinson(modelOrder, x.covariation))

  }

}
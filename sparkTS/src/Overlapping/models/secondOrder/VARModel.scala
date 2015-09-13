package overlapping.models.secondOrder

import breeze.linalg._
import org.apache.spark.rdd.RDD
import overlapping.IntervalSize
import overlapping.containers.block.{ColumnFirstBlock, SingleAxisBlock}
import overlapping.models.secondOrder.procedures.DurbinLevinson

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
class VARModel[IndexT <: Ordered[IndexT] : ClassTag](deltaT: Double, modelOrder: Int)
  extends AutoCovariances[IndexT](deltaT, modelOrder){

  override def estimate(slice: Array[(IndexT, Array[Double])]): Array[Signature] = {

    super
      .estimate(slice)
      .map(x => DurbinLevinson(modelOrder, x.covariation))

  }

  override def estimate(timeSeries: SingleAxisBlock[IndexT, Array[Double]]): Array[Signature] = {

    super
      .estimate(timeSeries)
      .map(x => DurbinLevinson(modelOrder, x.covariation))

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, Array[Double]])]): Array[Signature]= {

    super
      .estimate(timeSeries)
      .map(x => DurbinLevinson(modelOrder, x.covariation))

  }

}
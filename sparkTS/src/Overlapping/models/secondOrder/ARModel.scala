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
class ARModel[IndexT <: Ordered[IndexT] : ClassTag](selectionSize: Double, modelOrder: Int)
  extends AutoCovariances[IndexT](selectionSize, modelOrder) with DurbinLevinson{

  override def estimate(slice: Array[(IndexT, Array[Double])]): Array[Signature] = {

    super
      .estimate(slice)
      .map(x => runDL(modelOrder, x.covariation))

  }

  override def estimate(timeSeries: SingleAxisBlock[IndexT, Array[Double]]): Array[Signature] = {

    super
      .estimate(timeSeries)
      .map(x => runDL(modelOrder, x.covariation))

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, Array[Double]])]): Array[Signature]= {

    super
      .estimate(timeSeries)
      .map(x => runDL(modelOrder, x.covariation))

  }

}
package overlapping.models.secondOrder

import breeze.linalg._
import org.apache.spark.rdd.RDD
import overlapping.containers.block.SingleAxisBlock
import overlapping.models.secondOrder.procedures.InnovationAlgo

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
class MAModel[IndexT <: Ordered[IndexT] : ClassTag](selectionSize: Double, modelOrder: Int)
  extends AutoCovariances[IndexT](selectionSize, modelOrder) with InnovationAlgo{

  override def estimate(slice: Array[(IndexT, Array[Double])]): Array[Signature] = {

    super
      .estimate(slice)
      .map(x => runIA(modelOrder, x.covariation))

  }

  override def estimate(timeSeries: SingleAxisBlock[IndexT, Array[Double]]): Array[Signature] = {

    super
      .estimate(timeSeries)
      .map(x => runIA(modelOrder, x.covariation))

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, Array[Double]])]): Array[Signature]= {

    super
      .estimate(timeSeries)
      .map(x => runIA(modelOrder, x.covariation))

  }


}
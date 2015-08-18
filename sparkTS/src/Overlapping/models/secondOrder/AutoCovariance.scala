package overlapping.models.secondOrder

import breeze.linalg._
import overlapping.IntervalSize
import overlapping.containers.block.SingleAxisBlock

/*

Independent auto covariance should be paired with a special block
with column first access for time series.

(TimeSeriesBatch block)

/**
 * Created by Francois Belletti on 7/10/15.
 */
class AutoCovariance[IndexT <: Ordered[IndexT]](range: (Double, Double))
  extends Serializable with SecondOrderModel[IndexT, Array[Double]]{

  override def estimate(timeSeries: SingleAxisBlock[IndexT, Array[Double]]): Array[Array[Double]] ={

    return timeSeries
      .sliding(Array(IntervalSize(range._1, range._2)))


  }


}

*/
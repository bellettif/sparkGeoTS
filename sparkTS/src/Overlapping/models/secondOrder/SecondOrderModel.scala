package overlapping.models.secondOrder

import org.apache.spark.rdd.RDD
import overlapping.containers.block.SingleAxisBlock


/**
 * Created by Francois Belletti on 7/10/15.
 */
abstract trait SecondOrderModel[IndexT <: Ordered[IndexT], ValueT]
  extends Serializable{

  /*
  For windowed computations.
   */
  def estimate(slice: Array[(IndexT, ValueT)]): Any = ???

  /*
  For computation on a single block.
   */
  def estimate(timeSeries: SingleAxisBlock[IndexT, ValueT]): Any = ???

  /*
  For computation on an entire time series.
   */
  def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, ValueT])]): Any = ???

}
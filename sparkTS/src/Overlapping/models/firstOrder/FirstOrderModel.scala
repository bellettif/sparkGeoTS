package overlapping.models.firstOrder

import org.apache.spark.rdd.RDD
import overlapping.containers.block.SingleAxisBlock


/**
 * Created by Francois Belletti on 7/10/15.
 */
abstract trait FirstOrderModel[IndexT <: Ordered[IndexT], ValueT]
  extends Serializable{

  /*
  For computation on an entire time series.
   */
  def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, ValueT])]): Any = ???

}
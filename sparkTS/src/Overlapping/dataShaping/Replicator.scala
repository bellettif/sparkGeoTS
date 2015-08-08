package overlapping.dataShaping

import org.apache.spark.Partitioner
import timeIndex.containers.TimeSeriesHelper.TSInstant

import scala.math._
import scala.reflect.ClassTag

/**
  * Created by Francois Belletti on 6/24/15.
  */
trait Replicator[KeyT, ValueT] extends Serializable{

  def replicate(k: KeyT, v: ValueT): TraversableOnce[((Int, Int, KeyT), ValueT)]

}

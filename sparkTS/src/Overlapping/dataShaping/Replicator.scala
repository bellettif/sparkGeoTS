package overlapping.dataShaping

import org.apache.spark.Partitioner
import timeIndex.containers.TimeSeriesHelper.TSInstant

import scala.math._
import scala.reflect.ClassTag

/**
  * Created by Francois Belletti on 6/24/15.
  */
trait Replicator[KeyT, ValueT]{

  case class ExtendedKey(partIdx: Int, origK: KeyT, isReplica: Boolean)
  case class ExtendedKeyValue(k: ExtendedKey, v: ValueT)

  def replicate(k: KeyT, v: ValueT): List[ExtendedKeyValue]

}

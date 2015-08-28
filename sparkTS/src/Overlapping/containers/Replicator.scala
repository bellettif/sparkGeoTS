package overlapping.containers

import org.apache.spark.Partitioner

import scala.math._
import scala.reflect.ClassTag

/**
  *  A replicator creates overlapping data in overlapping blocks
  *  with respect to a certain partitioning scheme.
  */
trait Replicator[KeyT, ValueT] extends Serializable{

  def replicate(k: KeyT, v: ValueT): TraversableOnce[((Int, Int, KeyT), ValueT)]

}

package overlapping

/**
  *  A replicator creates overlapping data in overlapping blocks
  *  with respect to a certain partitioning scheme.
  */
trait Replicator[KeyT, ValueT] extends Serializable{

  def replicate(k: KeyT, v: ValueT): TraversableOnce[((Int, Int, KeyT), ValueT)]

}

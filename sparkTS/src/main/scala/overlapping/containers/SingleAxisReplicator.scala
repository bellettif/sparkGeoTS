package main.scala.overlapping.containers

import main.scala.overlapping.Replicator

import scala.reflect.ClassTag

/**
 * Implementation of the replicator in the case of an ordered index as in time series.
 */
class SingleAxisReplicator[IndexT : Ordering : ClassTag, ValueT: ClassTag](
    val intervals: Array[(IndexT, IndexT)],
    val selection: (IndexT, IndexT) => Boolean) extends Replicator[IndexT, ValueT]{

  /**
   * Get the index of the interval the timestamp belongs to.
   *
   * @param t Timestamp
   * @return Index of the interval
   */
  def getIntervalIdx(t: IndexT): Int ={

    val firstIdx = intervals.apply(0)._1
    if (implicitly[Ordering[IndexT]].compare(t, firstIdx) < 0) {
      return 0
    }

    for(((intervalStart, intervalEnd), intervalIdx)  <- intervals.zipWithIndex) {
      if ((implicitly[Ordering[IndexT]].compare(t, intervalStart) >= 0) &&
        (implicitly[Ordering[IndexT]].compare(t, intervalEnd) <= 0)) {
        return intervalIdx
      }
    }

    intervals.length - 1
  }

  /**
   * Create replicas if need be.
   *
   * @param k Original key (generally unique but not necessary)
   * @param v Original value
   * @return A sequence of ((origin partition, current partition, k), v)
   */
  override def replicate(k: IndexT, v: ValueT): Iterator[((Int, Int, IndexT), ValueT)] = {

    val i = getIntervalIdx(k)

    var result = ((i, i, k), v) :: Nil

    if((i > 0) && selection(intervals(i)._1, k)){
      result = ((i - 1, i, k), v) :: result
    }

    if((i < intervals.length - 1) && selection(intervals(i)._2, k)){
      result = ((i + 1, i, k), v) :: result
    }

    result.toIterator
  }


}

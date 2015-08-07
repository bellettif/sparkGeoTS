package overlapping.dataShaping.block

import overlapping.dataShaping.Replicator
import overlapping.dataShaping.graph.GroupLinearGraph
import timeIndex.containers.TimeSeriesHelper._

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 8/6/15.
 */

case class TSKey(timestamp: TSInstant, sensorId: Long)

class TimeSeriesReplicator[DataT: ClassTag](intervals: Array[(TSInstant, TSInstant)],
                                paddingMillis: Long,
                                groupGraph:GroupLinearGraph[Int])
  extends Replicator[TSKey, DataT]{

  case class IntervalLocation(intervalIdx: Int, offsetMillis: Long, lookAheadMillis: Long)

  def getIntervalIdx(timestamp: TSInstant): Int ={

    val firstTimestamp = intervals.apply(0)._1
    if (timestamp.isBefore(firstTimestamp)) {
      return 0
    }

    for(((intervalStart, intervalEnd), intervalIdx)  <- intervals.zipWithIndex) {
      if ((!timestamp.isBefore(intervalStart)) && (!timestamp.isAfter(intervalEnd))) {
        return intervalIdx
      }
    }

    intervals.length - 1
  }

  def getIntervalLocation(timestamp: TSInstant): IntervalLocation ={

    val (firstTimestamp, _) = intervals.head
    if (timestamp.isBefore(firstTimestamp)) {
      return IntervalLocation(
        0,
        timestamp.getMillis - firstTimestamp.getMillis, // This offset will be negative
        firstTimestamp.getMillis - timestamp.getMillis)
    }

    for(((intervalStart, intervalEnd), intervalIdx)  <- intervals.zipWithIndex) {
      if ((!timestamp.isBefore(intervalStart)) && (!timestamp.isAfter(intervalEnd))) {
        return IntervalLocation(
          intervalIdx,
          timestamp.getMillis - intervalStart.getMillis,
          intervalEnd.getMillis - timestamp.getMillis)
      }
    }

    val (_, lastTimestamp) = intervals.last
    IntervalLocation(
      intervals.length - 1,
      timestamp.getMillis - lastTimestamp.getMillis,
      lastTimestamp.getMillis - timestamp.getMillis) // This look ahead will be negative
  }

  override def replicate(k: TSKey, v: DataT): List[ExtendedKeyValue] = {

    val intervalLocation = getIntervalLocation(k.timestamp)

    var result = ExtendedKeyValue(ExtendedKey(intervalLocation.intervalIdx - 1, k, false), v) :: Nil

    if((intervalLocation.offsetMillis < paddingMillis) && (intervalLocation.intervalIdx > 0)){
      result = ExtendedKeyValue(ExtendedKey(intervalLocation.intervalIdx - 1, k, true), v) :: result
    }

    if((intervalLocation.offsetMillis < paddingMillis) && (intervalLocation.intervalIdx < intervals.size - 1)){
      result = ExtendedKeyValue(ExtendedKey(intervalLocation.intervalIdx + 1, k, true), v) :: result
    }

    result
  }


}

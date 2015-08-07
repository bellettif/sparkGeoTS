package overlapping.partitioners

import timeIndex.containers.TimeSeriesHelper._

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 8/6/15.
 */

case class TSKey(timestamp: TSInstant, sensorId: Long)

class TimeSeriesTableReplicator[DataT: ClassTag](intervals: Seq[(TSInstant, TSInstant)],
                                paddingMillis: Long,
                                groupGraph:GroupLinearGraph[Int])
  extends Replicator[TSKey, DataT]{

  case class IntervalLocation(intervalIdx: Int, offsetMillis: Long, lookAheadMillis: Long)

  def getIntervalIdx(timestamp: TSInstant): Int ={
    for(((intervalStart, intervalEnd), intervalIdx)  <- intervals.zipWithIndex) {
      if ((!timestamp.isBefore(intervalStart)) && (!timestamp.isAfter(intervalEnd))) {
        return intervalIdx
      }
    }
    throw new IndexOutOfBoundsException("Invalid timestamp" + timestamp.toString)
  }

  def getIntervalLocation(timestamp: TSInstant): IntervalLocation ={
    for(((intervalStart, intervalEnd), intervalIdx)  <- intervals.zipWithIndex) {
      if ((!timestamp.isBefore(intervalStart)) && (!timestamp.isAfter(intervalEnd))) {
        return IntervalLocation(intervalIdx,
          timestamp.getMillis - intervalStart.getMillis,
          intervalEnd.getMillis - timestamp.getMillis)
      }
    }
    throw new IndexOutOfBoundsException("Invalid timestamp" + timestamp.toString)
  }

  override def replicate(k: TSKey, v: DataT): List[ExtendedKeyValue] = {
    val intervalLocation = getIntervalLocation(k.timestamp)
    var result = ExtendedKeyValue(ExtendedKey(intervalLocation.intervalIdx - 1, k), v) :: Nil
    if((intervalLocation.offsetMillis < paddingMillis) && (intervalLocation.intervalIdx > 0)){
      result = ExtendedKeyValue(ExtendedKey(intervalLocation.intervalIdx - 1, k), v) :: result
    }
    if((intervalLocation.offsetMillis < paddingMillis) && (intervalLocation.intervalIdx < intervals.size - 1)){
      result = ExtendedKeyValue(ExtendedKey(intervalLocation.intervalIdx + 1, k), v) :: result
    }
    result
  }


}

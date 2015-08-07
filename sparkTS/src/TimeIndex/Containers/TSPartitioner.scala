package timeIndex.containers

import timeIndex.containers.TimeSeriesHelper.TSInstant
import org.apache.spark.Partitioner

import scala.math._

/**
 * Created by Francois Belletti on 6/24/15.
 */
class TSPartitioner(override val numPartitions: Int,
                    partitionToSizeAndInterval: Map[Int, (Int, TSInstant, TSInstant)])
  extends Partitioner{

  implicit val TSInstantReverseOrdering = new Ordering[((Int, TSInstant, TSInstant), Int)] {
    override def compare(a: ((Int, TSInstant, TSInstant), Int), b: ((Int, TSInstant, TSInstant), Int)) =
      a._1._2.compareTo(b._1._2)
  }

  // Intervals sorted in increasing order
  val sizeAndIntervalToPartition = partitionToSizeAndInterval
    .map(_.swap)
    .toArray
    .sorted

  def getPartitionIdx(timestamp: TSInstant): Int ={
    for(((_, intervalStart, intervalEnd), partIdx1)  <- sizeAndIntervalToPartition) {
      if ((!timestamp.isBefore(intervalStart)) && (!timestamp.isAfter(intervalEnd))) {
        return partIdx1
      }
    }
    throw new IndexOutOfBoundsException("Invalid timestamp " + timestamp.toString)
  }

  def getLastTimestampOfPartition(partIdx: Int): TSInstant ={
    val queryResult = partitionToSizeAndInterval.get(partIdx)
    if(queryResult.isDefined)
      queryResult.get._3
    else throw new IndexOutOfBoundsException("Invalid partition index " + partIdx)
  }

  def getLastTimestampOfPartition(timestamp: TSInstant): TSInstant ={
    return getLastTimestampOfPartition(getPartitionIdx(timestamp))
  }

  def getPartitionOffset(timestamp: TSInstant): Long ={
    for(((_, intervalStart, intervalEnd), partIdx1)  <- sizeAndIntervalToPartition) {
      if ((!timestamp.isBefore(intervalStart)) && (!timestamp.isAfter(intervalEnd))) {
        return timestamp.getMillis - intervalStart.getMillis
      }
    }
    throw new IndexOutOfBoundsException("Invalid timestamp" + timestamp.toString)
  }

  def getPartitionSize(partIdx: Int): Int={
    val queryResult = partitionToSizeAndInterval.get(partIdx)
    if(queryResult.isDefined)
      queryResult.get._1
    else throw new IndexOutOfBoundsException("Invalid partition index " + partIdx)
  }

  def getPartition(key: Any): Int = key match {
    case key: Int => key
  }


}

package TsUtils

import TsUtils.TimeSeriesHelper.TSInstant
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.joda.time.ReadableInstant

import scala.math._
import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 6/24/15.
 */
class TSPartitioner(override val numPartitions: Int,
                    partitionToSizeAndIntervalStart: Map[Int, (Int, TSInstant)])
  extends Partitioner{

  implicit val TSInstantReverseOrdering = new Ordering[((Int, TSInstant), Int)] {
    override def compare(a: ((Int, TSInstant), Int), b: ((Int, TSInstant), Int)) =
      b._1._2.compareTo(a._1._2)
  }

  val reverseIntervalStartPartition = partitionToSizeAndIntervalStart
    .map(_.swap)
    .toArray
    .sorted

  val reverseIntervalPartition = reverseIntervalStartPartition.drop(1) zip reverseIntervalStartPartition

  println()

  def getPartitionIdx(timestamp: TSInstant): Int ={
    if(! timestamp.isBefore(reverseIntervalStartPartition(0)._1._2)) {
      return reverseIntervalStartPartition(0)._2
    }
    for((((_, intervalStart), partIdx1), ((_, intervalEnd), partIdx2))  <- reverseIntervalPartition) {
      if ((!timestamp.isBefore(intervalStart)) && timestamp.isBefore(intervalEnd)) {
        return partIdx1
      }
    }
    throw new IndexOutOfBoundsException("Invalid timestamp " + timestamp.toString)
  }

  def getPartitionOffset(timestamp: TSInstant): Long ={
    if(! timestamp.isBefore(reverseIntervalStartPartition(0)._1._2)) {
      return timestamp.getMillis - reverseIntervalStartPartition(0)._1._2.getMillis
    }
    for((((_, intervalStart), partIdx1), ((_, intervalEnd), partIdx2))  <- reverseIntervalPartition) {
      if ((!timestamp.isBefore(intervalStart)) && timestamp.isBefore(intervalEnd)) {
        return timestamp.getMillis - intervalStart.getMillis
      }
    }
    throw new IndexOutOfBoundsException("Invalid timestamp " + timestamp.toString)
  }

  def getPartitionSize(partIdx: Int): Int={
    partitionToSizeAndIntervalStart(partIdx)._1
  }

  def getPartition(key: Any): Int = key match {
    case key: Int => key
  }


}

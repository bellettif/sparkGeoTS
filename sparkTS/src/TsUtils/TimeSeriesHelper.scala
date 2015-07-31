package TsUtils

import org.apache.spark.{RangePartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Interval}

import scala.math._
import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/28/15.
 */
object TimeSeriesHelper extends Serializable{

  type TSInstant  = DateTime
  type TSInterval = Interval

  def buildTilesFromSyncData[RawType: ClassTag, RecordType: ClassTag](
         config: TimeSeriesConfig,
         rawRDD: RDD[RawType],
         timeExtractor: RawType => (TSInstant, Array[RecordType])):
  (RDD[Array[RecordType]], RDD[TSInstant])= {

    val partitionDuration   = config.partitionDuration
    val nPartitions         = config.nPartitions
    val memory              = config.memory
    val partitioner         = config.partitioner

    /*
    Convert the time index to longs
   */
    implicit val RecordTimeOrdering = new Ordering[(TSInstant, Array[RecordType])] {
      override def compare(a: (TSInstant, Array[RecordType]), b: (TSInstant, Array[RecordType])) =
        a._1.compareTo(b._1)
    }

    implicit val TSInstantOrdering = new Ordering[TSInstant] {
      override def compare(a: TSInstant, b: TSInstant) =
        a.compareTo(b)
    }

    val timeRDD: RDD[(TSInstant, Array[RecordType])] = {
      val tempRDD: RDD[(TSInstant, Array[RecordType])]  = rawRDD
        .map(timeExtractor)
        //.map({case (t, v) => (t.getMillis, v)})
      val rangePartitioner: Partitioner = new RangePartitioner(nPartitions.value, tempRDD)
      tempRDD.repartitionAndSortWithinPartitions(rangePartitioner)
    }

    /*
    Figure out the time partitioner
     */
    def stitchAndTranspose(kVPairs: Iterator[(Long, Array[RecordType])]): Iterator[Array[RecordType]] ={
      kVPairs.toSeq.map(_._2).transpose.map(x => Array(x: _*)).iterator
    }

    def computePartitionOffset(t: TSInstant): Long ={
      t.getMillis % partitionDuration.value
    }

    def computePartititionIndex(t: TSInstant): Long ={
      floor(t.getMillis / partitionDuration.value).toLong
    }


    /*
    Gather data into overlapping tiles
     */

    val rowOrientatedRDD = timeRDD
      .flatMap({case (t, v) =>
      if ((computePartitionOffset(t) <= memory.value) &&
        (computePartititionIndex(t) > 0))
      // Create the overlap
      // ((partition, timestamp), record)
        (computePartititionIndex(t), v) :: (computePartititionIndex(t) - 1, v) :: Nil
      else
      // Outside the overlapping region
        (computePartititionIndex(t), v) :: Nil
    })
      .partitionBy(partitioner)

    /*
    #############################################
            CONTAINERS IN THEIR FINAL FORM
    #############################################
     */

    val tiles = rowOrientatedRDD
      .mapPartitions(stitchAndTranspose, true)

    val timeStamps: RDD[TSInstant] = timeRDD
      .flatMap({case (t, v) =>
      if ((computePartitionOffset(t) <= memory.value) &&
        (computePartititionIndex(t) > 0))
      // Create the overlap
      // ((partition, timestamp), record)
        (computePartititionIndex(t), t) :: (computePartititionIndex(t) - 1, t) :: Nil
      else
      // Outside the overlapping region
        (computePartititionIndex(t), t) :: Nil
    })
      .partitionBy(partitioner)
      .mapPartitions(_.map(_._2), true)

    (tiles, timeStamps)


  }


}

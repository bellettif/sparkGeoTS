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
  (TSPartitioner, RDD[Array[RecordType]], RDD[TSInstant])= {

    val partitionDuration   = config.partitionDuration
    val nPartitions         = config.nPartitions
    val memory              = config.memory

    /*
    Convert the time index to longs
   */
    implicit val TSInstantOrdering = new Ordering[TSInstant] {
      override def compare(a: TSInstant, b: TSInstant) =
        a.compareTo(b)
    }

    /*
      Parse each row from RawType to (TSInstant, Array[RecordType])
     */
    val sortedWithinPartitionsParsedRDD: RDD[(TSInstant, Array[RecordType])] = {
      val tempRDD: RDD[(TSInstant, Array[RecordType])]  = rawRDD
        .map(timeExtractor)
      val rangePartitioner: Partitioner = new RangePartitioner(nPartitions.value, tempRDD)
      tempRDD.repartitionAndSortWithinPartitions(rangePartitioner)
    }

    /*
      Map each partition to its time interval, this will be used in the TS partitioning
     */
    def extractSizeAndFirstElement[T: ClassTag](iterator: Iterator[(TSInstant, T)]): (Int, TSInstant) = {
      val (it1, it2) = iterator.duplicate
      return (it1.size, it2.take(1).toArray.apply(0)._1)
    }

    val partitionToSizeAndIntervalStart = sortedWithinPartitionsParsedRDD
      .mapPartitionsWithIndex({case (partIdx, partContent)
                                => Seq((partIdx, extractSizeAndFirstElement(partContent))).toIterator},
                                true)
      .collectAsMap
      .toMap

    /*
      This is the TS partitioner that will be used in the end
     */
    val partitioner = new TSPartitioner(nPartitions.value, partitionToSizeAndIntervalStart)

    def stitchAndTranspose(kVPairs: Iterator[(Int, Array[RecordType])]): Iterator[Array[RecordType]] ={
      kVPairs.toSeq.map(_._2).transpose.map(x => Array(x: _*)).iterator
    }

    def computePartititionIndex = partitioner.getPartitionIdx _

    def computePartitionOffset = partitioner.getPartitionOffset _

    /*
    Gather data into overlapping tiles
     */

    val rowOrientatedRDD = sortedWithinPartitionsParsedRDD
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

    val temp = tiles.glom().collect()

    println()

    val timeStamps: RDD[TSInstant] = sortedWithinPartitionsParsedRDD
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

    (partitioner, tiles, timeStamps)


  }


}

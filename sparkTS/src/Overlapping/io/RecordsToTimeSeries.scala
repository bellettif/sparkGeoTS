package overlapping.io

import overlapping.BlockGraph
import overlapping.partitioners.BlockGraphPartitioner
import timeIndex.containers.{TSPartitioner, TimeSeriesConfig}
import org.apache.spark.{RangePartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.joda.time.{Interval, DateTime}

import scala.math.Ordering
import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 8/6/15.
 */
object RecordsToTimeSeries {

  def transposeData[DataT, KeyT, IndexT](timePaddingMillis: Long,
                                         nPartitions: Int,
                                         recordRDD: RDD[(KeyT, DataT)])
                                        (implicit bgp: BlockGraphPartitioner[KeyT, IndexT],
                                         keyOrdering: Ordering[IndexT]):
    RDD[(((IndexT, IndexT), Long), BlockGraph[KeyT, DataT])]= {

    /*
      Sort the record RDD with respect to time
     */
    val rangePartitioner: Partitioner = new RangePartitioner(nPartitions, recordRDD)(keyOrdering)

    val sortedWithinPartitionsParsedRDD: RDD[(IndexT, (KeyT, DataT))] = recordRDD
      .map({case (k, v) => (bgp.indexExtractor(k), (k, v))})
      .repartitionAndSortWithinPartitions(rangePartitioner)

    /*
      Map each partition to its time interval, this will be used in the TS partitioning
     */
    def extractSizeFirstAndLast[T: ClassTag](iterator: Iterator[(, T)]): (Int, , ) = {
      val (it1, it2) = iterator.duplicate
      val (it3, it4) = it2.duplicate
      var lastElement = it4.next()
      while(it4.hasNext){
        lastElement = it4.next()
      }
      return (it1.size, it3.next()._1, lastElement._1)
    }

    val partitionToSizeAndInterval = sortedWithinPartitionsParsedRDD
      .mapPartitionsWithIndex({case (partIdx, partContent)
    => Seq((partIdx, extractSizeFirstAndLast(partContent))).toIterator},
    true)
      .collectAsMap
      .toMap

    /*
      This is the TS partitioner that will be used in the end
     */
    val partitioner = new TSPartitioner(nPartitions.value, partitionToSizeAndInterval)

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


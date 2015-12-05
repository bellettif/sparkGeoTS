package main.scala.overlapping.containers

import breeze.linalg.{min, DenseVector}
import breeze.numerics.{log, sqrt}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag

/**
 * This object is used to transform an RDD of raw unsorted data into
 * an RDD of key value pairs where keys are partition indices and values
 * are SingleAxisBlocks inside which the data is sorted.
 */
object SingleAxisBlockRDD {

  /**
   * This will devise approximatively balanced intervals to partition the raw data along.
   * Partitions will be created, overlaps materialized and the data within each block will be sorted.
   *
   * @param selection Criterion for two timestamps to belong in the same partition.
   *                  Anchor is left timestamp (e.g. bound of an interval).
   * @param nPartitions Number of partitions desired.
   * @param recordRDD Input data.
   * @tparam IndexT Timestamp type.
   * @tparam ValueT Data type.
   * @return The resulting main.scala.overlapping block RDD and and array of intervals (begin, end).
   */
  def apply[IndexT <: Ordered[IndexT] : ClassTag, ValueT: ClassTag](
      selection: (IndexT, IndexT) => Boolean,
      nPartitions: Int,
      recordRDD: RDD[(IndexT, ValueT)]): (RDD[(Int, SingleAxisBlock[IndexT, ValueT])], Array[(IndexT, IndexT)]) = {

    val nSamples = recordRDD.count()

    val intervals = IntervalSampler
      .sampleAndComputeIntervals(
        nPartitions,
        sqrt(nSamples).toInt,
        recordRDD,
        Some(nSamples))

    val replicator = new SingleAxisReplicator[IndexT, ValueT](intervals, selection)
    val partitioner = new BlockIndexPartitioner(intervals.length)

    //println("Replicator and partitioner set up done.")

    (recordRDD
      .flatMap({ case (k, v) => replicator.replicate(k, v) })
      .partitionBy(partitioner)
      .mapPartitionsWithIndex({case (i, x) => ((i, SingleAxisBlock(x.toArray)) :: Nil).toIterator}, true)
    ,intervals)

  }

  /**
   * This is to build an RDD with predefined partitioning intervals.
   * This is useful so that two OverlappingBlock RDDs have
   * corresponding main.scala.overlapping blocks mapped on the same key.
   *
   * @param padding Backward and forward padding in distance metric (usually milli seconds).
   * @param signedDistance The distance to use.
   * @param intervals
   * @param recordRDD
   * @tparam IndexT
   * @tparam ValueT
   * @return
   */
  def fromIntervals[DistanceT, IndexT <: Ordered[IndexT], ValueT: ClassTag](
      padding: (DistanceT, DistanceT),
      intervals: Array[(IndexT, IndexT)],
      recordRDD: RDD[(IndexT, ValueT)])
      (implicit signedDistance: (IndexT, IndexT) => DistanceT): RDD[(Int, SingleAxisBlock[IndexT, ValueT])] = {

    case class KeyValue(k: IndexT, v: ValueT)

    val replicator = new SingleAxisReplicator[IndexT, ValueT](intervals, signedDistance, padding)
    val partitioner = new BlockIndexPartitioner(intervals.length)

    recordRDD
      .flatMap({ case (k, v) => replicator.replicate(k, v) })
      .partitionBy(partitioner)
      .mapPartitionsWithIndex({case (i, x) => ((i, SingleAxisBlock(x.toArray)) :: Nil).toIterator}, true)

  }

}


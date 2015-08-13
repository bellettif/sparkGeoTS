package overlapping.io

import breeze.numerics.sqrt
import org.joda.time.{Interval, DateTime}
import overlapping.BlockGraph
import org.apache.spark.{RangePartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import overlapping.dataShaping.block.{SingleAxisBlock, SingleAxisReplicator, BlockIndexPartitioner, IntervalSampler}

import scala.math.Ordering
import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 8/6/15.
 */
object SingleAxisBlockRDD {

  def apply[IndexT <: Ordered[IndexT], ValueT: ClassTag](padding: (Double, Double),
                                                         signedDistance: (IndexT, IndexT) => Double,
                                                         nPartitions: Int,
                                                         recordRDD: RDD[(IndexT, ValueT)]): RDD[(Int, SingleAxisBlock[IndexT, ValueT])] = {

    case class KeyValue(k: IndexT, v: ValueT)
    /*
      Sort the record RDD with respect to time
     */
    implicit val kvOrdering = new Ordering[(IndexT, ValueT)] {
      override def compare(a: (IndexT, ValueT), b: (IndexT, ValueT)) =
        a._1.compareTo(b._1)
    }

    val nSamples = recordRDD.count()

    val intervals = IntervalSampler
      .sampleAndComputeIntervals(
        nPartitions,
        sqrt(nSamples).toInt,
        true,
        recordRDD)
      .map({ case ((k1, v1), (k2, v2)) => (k1, k2) })

    val replicator = new SingleAxisReplicator[IndexT, ValueT](intervals, signedDistance, padding)
    val partitioner = new BlockIndexPartitioner(intervals.length)

    recordRDD
      .flatMap({ case (k, v) => replicator.replicate(k, v) })
      .partitionBy(partitioner)
      .mapPartitionsWithIndex({case (i, x) => ((i, SingleAxisBlock(x.toArray, signedDistance)) :: Nil).toIterator}, true)

  }

}


package overlapping.io

import breeze.numerics.sqrt
import org.joda.time.{Interval, DateTime}
import overlapping.BlockGraph
import org.apache.spark.{RangePartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import overlapping.dataShaping.block.{SingleAxisReplicator, BlockIndexPartitioner, IntervalSampler}

import scala.math.Ordering
import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 8/6/15.
 */
object RecordsToTimeSeries {

  case class TSInstant(timestamp: DateTime) extends Ordered[TSInstant]{
    def compare(that: TSInstant): Int = {
      this.timestamp.compareTo(that.timestamp)
    }
  }
  def transposeData[DataT: ClassTag](paddingMillis: Long,
                                     nPartitions: Int,
                                     recordRDD: RDD[(TSInstant, DataT)])= {

    case class KeyValue(k: TSInstant, v: DataT)
    /*
      Sort the record RDD with respect to time
     */
    implicit val DateTimeOrdering = new Ordering[(TSInstant, DataT)] {
      override def compare(a: (TSInstant, DataT), b: (TSInstant, DataT)) =
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

    val deltaMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val replicator = new SingleAxisReplicator[TSInstant, DataT](intervals, deltaMillis, paddingMillis.toDouble)
    val partitioner = new BlockIndexPartitioner(intervals.length)

    val overlappingRDD = recordRDD
      .flatMap({ case (k, v) => replicator.replicate(k, v) })
      .partitionBy(partitioner)

    val temp = overlappingRDD.glom.collect

    print("Done")

  }

}


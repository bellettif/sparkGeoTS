package overlapping.io

import breeze.numerics.sqrt
import org.joda.time.{Interval, DateTime}
import overlapping.BlockGraph
import org.apache.spark.{RangePartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import overlapping.dataShaping.block.{IntervalSampler, TimeSeriesReplicator}

import scala.math.Ordering
import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 8/6/15.
 */
object RecordsToTimeSeries {

  def transposeData[DataT: ClassTag](timePaddingMillis: Long,
                                     nPartitions: Int,
                                     recordRDD: RDD[(DateTime, DataT)])= {

    case class KeyValue(k: DateTime, v: DataT)
    /*
      Sort the record RDD with respect to time
     */
    implicit val DateTimeOrdering = new Ordering[(DateTime, DataT)] {
      override def compare(a: (DateTime, DataT), b: (DateTime, DataT)) =
        a._1.compareTo(b._1)
    }

    val nSamples = recordRDD.count()

    val intervals = IntervalSampler.sampleAndComputeIntervals(
      nPartitions,
      sqrt(nSamples).toInt,
      true,
      recordRDD)

    println("Done")

    }

}


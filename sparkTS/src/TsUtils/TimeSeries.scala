package TsUtils

import scala.reflect._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.OrderedRDDFunctions
import org.apache.spark.SparkContext

import org.joda.time.DateTime

import scala.math._

/**
 * Created by Francois Belletti on 6/24/15.
 */
class TimeSeries[RawType: ClassTag, RecordType: ClassTag](rawRDD: RDD[RawType],
                  timeExtract: RawType => (Long, Array[RecordType]),
                  sc: SparkContext,
                  memory: Option[Int]) extends Serializable{

  var effectiveLag = sc.broadcast(if (memory.isDefined) memory.get else 10)

  val timeRDD: RDD[(Long, Array[RecordType])] = {
    val tempRDD: RDD[(Long, Array[RecordType])]  = rawRDD.map(timeExtract)
    val partitioner: Partitioner = new RangePartitioner(100, tempRDD)
    tempRDD.repartitionAndSortWithinPartitions(partitioner)
  }

  implicit val RecordTimeOrdering = new Ordering[(Long, Array[RecordType])] {
    override def compare(a: (Long, Array[RecordType]), b: (Long, Array[RecordType])) =
      a._1.compare(b._1)
  }

  val minTS = sc.broadcast(timeRDD.min()._1)
  val maxTS = sc.broadcast(timeRDD.max()._1)
  val nTS   = sc.broadcast(maxTS.value - minTS.value)
  val nCols = sc.broadcast(10)

  val partitionLength = sc.broadcast(effectiveLag.value * 2)
  val nPartitions     = sc.broadcast(floor(nTS.value / partitionLength.value).toInt + 1)

  val partitioner = new TSPartitioner(nPartitions.value)

  def stitchAndTranspose(kVPairs: Iterator[((Int, Long), Array[RecordType])]): Iterator[Array[RecordType]] ={
    def col(i: Int) = for (kVPair <- kVPairs) yield kVPair._2.apply(i)
    val range: Range = (0 until nCols.value)
    for (i <- range.iterator) yield Array(col(i).toSeq: _*)
  }

  val augmentedIndexRDD = timeRDD
    .flatMap({case (t, v) =>
    if (((t - minTS.value) % partitionLength.value <= effectiveLag.value) &&
        (floor((t - minTS.value) / partitionLength.value).toInt > 0))
      ((floor((t - minTS.value) / partitionLength.value).toInt, t), v)::
        ((floor((t - minTS.value) / partitionLength.value).toInt - 1, t), v)::
        Nil
    else
      ((floor((t - minTS.value) / partitionLength.value).toInt, t), v)::Nil
  }).partitionBy(partitioner)
  .mapPartitions(stitchAndTranspose, true)



}

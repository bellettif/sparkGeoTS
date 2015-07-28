package TsUtils

import org.apache.spark.{RangePartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import scala.math._
import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/28/15.
 */
object TimeSeriesHelper extends Serializable{

  def buildTilesFromSyncData[RawType: ClassTag, RecordType: ClassTag](
         timeSeries: TimeSeries[RawType, RecordType],
         rawRDD: RDD[RawType],
         timeExtractor: RawType => (DateTime, Array[RecordType])) = {

    val partitionLength = timeSeries.partitionLength
    val nPartitions     = timeSeries.nPartitions
    val effectiveLag    = timeSeries.effectiveLag
    val partitioner     = timeSeries.partitioner

    /*
  Convert the time index to longs
   */
    implicit val RecordTimeOrdering = new Ordering[(Long, Array[RecordType])] {
      override def compare(a: (Long, Array[RecordType]), b: (Long, Array[RecordType])) =
        a._1.compare(b._1)
    }

    val timeRDD: RDD[(Long, Array[RecordType])] = {
      val tempRDD: RDD[(Long, Array[RecordType])]  = rawRDD
        .map(timeExtractor)
        .map({case (t, v) => (t.getMillis, v)})
      val partitioner: Partitioner = new RangePartitioner(nPartitions.value, tempRDD)
      tempRDD.repartitionAndSortWithinPartitions(partitioner)
    }

    /*
    Figure out the time partitioner
     */
    def stitchAndTranspose(kVPairs: Iterator[((Int, Long), Array[RecordType])]): Iterator[Array[RecordType]] ={
      kVPairs.toSeq.map(_._2).transpose.map(x => Array(x: _*)).iterator
    }
    def extractTimeIndex(kVPairs: Iterator[((Int, Long), Array[RecordType])]): Iterator[Long] ={
      kVPairs.toSeq.map(_._1._2).iterator
    }

    /*
    Gather data into overlapping tiles
     */

    val augmentedIndexRDD = timeRDD
      .zipWithIndex()
      .flatMap({case ((t, v), i) =>
      if ((i % partitionLength.value <= effectiveLag.value) &&
        (floor(i / partitionLength.value).toInt > 0))
      // Create the overlap
      // ((partition, timestamp), record)
        ((floor(i / partitionLength.value).toInt, t), v)::
          ((floor(i / partitionLength.value).toInt - 1, t), v)::
          Nil
      else
      // Outside the overlapping region
        ((floor(i / partitionLength.value).toInt, t), v)::Nil
    })
      .partitionBy(partitioner)

    /*
    #############################################

            CONTAINERS IN THEIR FINAL FORM

    #############################################
     */


    val tiles = augmentedIndexRDD
      .mapPartitions(stitchAndTranspose, true)

    val timeStamps: RDD[(Int, (Long, DateTime))] = rawRDD
      .map(x => timeExtractor(x)._1)
      .zipWithIndex()
      .flatMap({ case (t, i) =>
      if ((i % partitionLength.value <= effectiveLag.value) &&
        (floor(i / partitionLength.value).toInt > 0))
        (floor(i / partitionLength.value).toInt, (t.getMillis, t)) ::
          (floor(i / partitionLength.value).toInt - 1, (t.getMillis, t)) ::
          Nil
      else
        (floor(i / partitionLength.value).toInt, (t.getMillis, t)) :: Nil
    }
    )
      .partitionBy(partitioner)

    (tiles, timeStamps)

  }


}

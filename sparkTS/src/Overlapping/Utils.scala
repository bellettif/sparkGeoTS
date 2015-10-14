package overlapping

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import overlapping.containers.SingleAxisBlock
import overlapping.timeSeries.TSInstant

/**
 * Created by Francois Belletti on 10/12/15.
 */
class Utils {

  type RawTS = RDD[(TSInstant, DenseVector[Double] )]

  type OverlappingTS = RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])]

  implicit def signedDistMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

  implicit def signedDistLong = (t1: Long, t2: Long) => (t2 - t1).toDouble


}

package TsUtils

import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.joda.time.ReadableInstant

/**
 * Created by Francois Belletti on 6/24/15.
 */
class TSPartitioner(override val numPartitions: Int)
  extends Partitioner{

  def getPartition(key: Any): Int = key match {
    case (p: Int, t: Long) => p
    case (p: Int, t: ReadableInstant) => p
    case i: Int   => i
    case i: Long  => i.toInt / numPartitions
    case _ => key.hashCode()
  }

}

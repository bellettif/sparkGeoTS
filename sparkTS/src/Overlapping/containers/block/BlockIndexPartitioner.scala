package overlapping.containers.block

import org.apache.spark.Partitioner

import scala.math._

/**
 * Created by Francois Belletti on 6/24/15.
 */
class BlockIndexPartitioner(override val numPartitions: Int)
  extends Partitioner{

  def getPartition(key: Any): Int = key match {
    case key: (Int, _, _) => key._1
  }

}

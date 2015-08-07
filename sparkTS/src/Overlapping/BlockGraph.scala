package overlapping

import org.apache.spark.rdd.RDD

/**
 * Created by Francois Belletti on 8/6/15.
 */
trait BlockGraph[LocationT, DataT] {

  def partitions: RDD[(Array[LocationT], OverlappingGraph[OverlappingBlock[LocationT, DataT]])]
  def transpose: GraphBlock[LocationT, DataT]

}

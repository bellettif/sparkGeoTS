package overlapping

import org.apache.spark.rdd.RDD
import overlapping.containers.block.OverlappingBlock
import overlapping.containers.graph.OverlappingGraph

/**
 * Created by Francois Belletti on 8/6/15.
 */
trait BlockGraph[LocationT, DataT] {

  def partitions: RDD[(Array[LocationT], OverlappingGraph[OverlappingBlock[LocationT, DataT]])]
  def transpose: GraphBlock[LocationT, DataT]

}

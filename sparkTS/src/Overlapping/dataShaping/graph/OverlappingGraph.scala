package overlapping.dataShaping.graph

/**
 * Created by Francois Belletti on 8/6/15.
 */
trait OverlappingGraph[T]{

  def vertex_ids: Array[Long]

  def get(vertex_id: Long): T
  def put(vertex_id: Long, newvalue: T): OverlappingGraph[T]
  def puti(vertex_id: Long, newvalue: T): Unit

  def getParentIds(vertex_id: Long): Iterator[Long]
  def getChildIds(vertex_id: Long): Iterator[Long]

}

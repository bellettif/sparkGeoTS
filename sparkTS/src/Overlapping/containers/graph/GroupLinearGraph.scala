package overlapping.containers.graph

/**
 * Created by Francois Belletti on 8/6/15.
 *
 * An example of relational graph between table columns where only adjacent columns
 * are connected.
 *
 */
class GroupLinearGraph[T](val columns: Array[T])
  extends GroupUndirectedGraph[T]{

  override def getNeighbors(vertexId: Int): Iterator[Int] = {
    ((vertexId - 1) % columns.length
      :: vertexId
      :: ((vertexId + 1) % columns.length)
      :: Nil)
      .toIterator
  }

}

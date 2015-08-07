package overlapping.partitioners

/**
 * Created by Francois Belletti on 8/6/15.
 *
 * An example of relational graph between table columns where only adjacent columns
 * are connected.
 *
 */
class GroupLinearGraph[T](val columns: Array[T])
  extends GroupUndirectedGraph{

  override def getNeighbors(vertexId: Int): Iterator[T] = {
    (columns.apply((vertexId - 1) % columns.length)
      :: columns.apply(vertexId)
      :: columns.apply((vertexId + 1) % columns.length)
      :: Nil)
      .toIterator
  }

}

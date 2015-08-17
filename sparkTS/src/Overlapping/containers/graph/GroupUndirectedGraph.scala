package overlapping.containers.graph

/**
 * Created by Francois Belletti on 8/6/15.
 */
trait GroupUndirectedGraph[T] {

  def getNeighbors(vertexId: Int): Iterator[Int]

}
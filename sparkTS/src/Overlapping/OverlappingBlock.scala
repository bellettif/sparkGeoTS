package Overlapping

/**
 * Created by Francois Belletti on 8/6/15.
 */
trait OverlappingBlock[LocationT, DataT]{

  def data: Array[DataT]
  def locations: Array[LocationT] // Can be evenly spaced or not

  def overlapIndexSpan: Array[Int]
  def overlapSpatialSpan: Array[Double]
  def distance: Array[(LocationT, LocationT => Double)]

  // Define the padding of the iterator in terms of T along each direction
  def toIterator(padding: Array[Double]): Iterator[DataT]

  // Define the padding in terms of number of records along each direction
  def toIterator(padding: Array[Int]): Iterator[DataT]

  // Zipping on collisions with another overlapping block whose indices are different
  // This is equivalent to a join but is expected to be implemented in a manner more adapted to the data.
  def zipOnCollisions[ThatDataT, ResultT](thatLookAhead: Array[Double], that: OverlappingBlock[LocationT, ThatDataT])
                               (f: ((LocationT, DataT), (LocationT, ThatDataT)) => ResultT) : Iterator[ResultT]

}

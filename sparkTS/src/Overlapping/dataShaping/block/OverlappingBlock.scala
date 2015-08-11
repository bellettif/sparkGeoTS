package overlapping.dataShaping.block

/**
 * Created by Francois Belletti on 8/6/15.
 */
trait OverlappingBlock[KeyT, ValueT] extends Serializable{

  case class CompleteLocation(partIdx: Int, originIdx: Int, k: KeyT)
  case class KernelSize(lookBack: Int, lookAhead: Int)
  case class IntervalSize(lookBack: Double, lookAhead: Double)

  def data: Array[ValueT]
  def locations: Array[CompleteLocation] // Can be evenly spaced or not
  def algebraicDistances: Array[((ValueT, ValueT) => Double)]

  def sliding(size: Array[KernelSize], stride: Array[Int]): Iterator[Array[ValueT]]

  def sliding(size: Array[IntervalSize]): Iterator[Array[(KeyT, ValueT)]]

  def toIterator(): Iterator[ValueT]

  // Define the offsets of the iterator in terms of T along each direction
  def toIterator(offsets: Array[Double]): Iterator[ValueT]

  // Define the offsets in terms of number of records along each direction
  def toIterator(offsets: Array[Int]): Iterator[ValueT]

  /*
  // Zipping on collisions with another overlapping block whose indices are different
  // This is equivalent to a join but is expected to be implemented in a manner more adapted to the data.
  def zipOnCollisions[ThatDataT, ResultT](thatLookAhead: Array[Double], that: OverlappingBlock[LocationT, ThatDataT])
                               (f: ((LocationT, ValueT), (LocationT, ThatDataT)) => ResultT) : Iterator[ResultT]
  */

}

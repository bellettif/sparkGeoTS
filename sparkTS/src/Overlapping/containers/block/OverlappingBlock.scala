package overlapping.containers.block

import overlapping.CompleteLocation

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 8/6/15.
 */
trait OverlappingBlock[KeyT, ValueT] extends Serializable{

  case class IntervalSize(lookBack: Double, lookAhead: Double)

  def data: Array[(KeyT, ValueT)]
  def locations: Array[CompleteLocation[KeyT]] // Can be evenly spaced or not
  def signedDistances: Array[((KeyT, KeyT) => Double)]

  def sliding(size: Array[IntervalSize]): OverlappingBlock[KeyT, Array[(KeyT, ValueT)]]

  def sliding(size: Array[IntervalSize],
              targets: Iterator[CompleteLocation[KeyT]]): OverlappingBlock[KeyT, Array[(KeyT, ValueT)]]

  def slidingWindow(cutPredicates: Array[(KeyT, KeyT) => Boolean]): OverlappingBlock[KeyT, Array[(KeyT, ValueT)]]

  def filter(p: (KeyT, ValueT) => Boolean)(size: Array[IntervalSize]):
      OverlappingBlock[KeyT, ValueT]

  def map[ResultT: ClassTag](f: (KeyT, ValueT) => ResultT): OverlappingBlock[KeyT, ResultT]

  def reduce(f: (ValueT, ValueT) => ValueT): ValueT

  def fold(zeroValue: ValueT)(op: (ValueT, ValueT) => ValueT): ValueT

  def toIterator: Iterator[(KeyT, ValueT)] // Guarantees there is no redundancy here

}

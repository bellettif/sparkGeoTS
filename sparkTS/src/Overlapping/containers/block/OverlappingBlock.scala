package overlapping.containers.block

import overlapping.{IntervalSize, CompleteLocation}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 8/6/15.
 */
trait OverlappingBlock[KeyT, ValueT] extends Serializable{

  def data: Array[(KeyT, ValueT)]
  def locations: Array[CompleteLocation[KeyT]] // Can be evenly spaced or not
  def signedDistances: Array[((KeyT, KeyT) => Double)]

  def sliding(size: Array[IntervalSize]): OverlappingBlock[KeyT, Array[(KeyT, ValueT)]]

  def sliding(size: Array[IntervalSize],
              targets: Array[CompleteLocation[KeyT]]): OverlappingBlock[KeyT, Array[(KeyT, ValueT)]]

  def slicingWindow(cutPredicates: Array[(KeyT, KeyT) => Boolean]): OverlappingBlock[KeyT, Array[(KeyT, ValueT)]]

  def filter(p: (KeyT, ValueT) => Boolean):
      OverlappingBlock[KeyT, ValueT]

  def map[ResultT: ClassTag](f: (KeyT, ValueT) => ResultT): OverlappingBlock[KeyT, ResultT]

  def reduce(f: ((KeyT, ValueT), (KeyT, ValueT)) => (KeyT, ValueT)): (KeyT, ValueT)

  def fold(zeroValue: (KeyT, ValueT))(op: ((KeyT, ValueT), (KeyT, ValueT)) => (KeyT, ValueT)): (KeyT, ValueT)

  def toArray: Array[(KeyT, ValueT)] // Guarantees there is no redundancy here

  def count: Long

}

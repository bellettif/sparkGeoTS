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

  def sliding[ResultT: ClassTag](size: Array[IntervalSize])
                                (f: Array[(KeyT, ValueT)] => ResultT): OverlappingBlock[KeyT, ResultT]

  def sliding[ResultT: ClassTag](size: Array[IntervalSize],
                                 targets: Array[CompleteLocation[KeyT]])
                                (f: Array[(KeyT, ValueT)] => ResultT): OverlappingBlock[KeyT, ResultT]

  def slicingWindow[ResultT: ClassTag](cutPredicates: Array[(KeyT, KeyT) => Boolean])
                                      (f: Array[(KeyT, ValueT)] => ResultT): OverlappingBlock[KeyT, ResultT]

  def slicingWindowFold[ResultT: ClassTag](cutPredicates: Array[(KeyT, KeyT) => Boolean])
                                          (f: Array[(KeyT, ValueT)] => ResultT,
                                           zero: ResultT,
                                           op: (ResultT, ResultT) => ResultT): ResultT

  def filter(p: (KeyT, ValueT) => Boolean): OverlappingBlock[KeyT, ValueT]

  def map[ResultT: ClassTag](f: (KeyT, ValueT) => ResultT): OverlappingBlock[KeyT, ResultT]

  def reduce(f: ((KeyT, ValueT), (KeyT, ValueT)) => (KeyT, ValueT)): (KeyT, ValueT)

  def fold(zeroValue: (KeyT, ValueT))(op: ((KeyT, ValueT), (KeyT, ValueT)) => (KeyT, ValueT)): (KeyT, ValueT)

  def slidingFold[ResultT: ClassTag](size: Array[IntervalSize],
                                     targets: Array[CompleteLocation[KeyT]])
                                    (f: Array[(KeyT, ValueT)] => ResultT,
                                     zero: ResultT,
                                     op: (ResultT, ResultT) => ResultT): ResultT

  def slidingFold[ResultT: ClassTag](size: Array[IntervalSize])
                                    (f: Array[(KeyT, ValueT)] => ResultT,
                                     zero: ResultT,
                                     op: (ResultT, ResultT) => ResultT): ResultT

  def toArray: Array[(KeyT, ValueT)] // Guarantees there is no redundancy here

  def count: Long

  def take(n: Int): Array[(KeyT, ValueT)]

}

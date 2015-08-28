package overlapping.containers.block

import overlapping.{IntervalSize, CompleteLocation}

import scala.reflect.ClassTag

/**
 *  Main trait of the overlapping weak memory data project.
 *  The distributed representation of data will be based on a key, value RDD of
 *  (int, OverlappingBlock) where the key is the index of the partition.
 */
trait OverlappingBlock[KeyT, ValueT] extends Serializable{

  /*
   Raw data
    */
  def data: Array[(KeyT, ValueT)]

  /*
   Locations that keep track of the current partition index and the original
   partition index. Redundant elements will have different partition index
   and original partition index.
    */
  def locations: Array[CompleteLocation[KeyT]] // Can be evenly spaced or not

  /*
   Array of distance functions (one per layout axis).
   */
  def signedDistances: Array[((KeyT, KeyT) => Double)]

  /*
  Apply the kernel f with a window width of size (array of sizes, one per layout axis),
  centering the kernel computation on the targets.
   */
  def sliding[ResultT: ClassTag](size: Array[IntervalSize],
                                 targets: Array[CompleteLocation[KeyT]])
                                (f: Array[(KeyT, ValueT)] => ResultT): OverlappingBlock[KeyT, ResultT]

  /*
  Roll apply the kernel f centering all computations on the admissible data points
  of the overlapping block.
   */
  def sliding[ResultT: ClassTag](size: Array[IntervalSize])
                                (f: Array[(KeyT, ValueT)] => ResultT): OverlappingBlock[KeyT, ResultT]

  /*
  Here there is no roll apply. An array of predicates decides on when to separate a new window
  in the data and compute the kernel f on it.
   */
  def slicingWindow[ResultT: ClassTag](cutPredicates: Array[(KeyT, KeyT) => Boolean])
                                      (f: Array[(KeyT, ValueT)] => ResultT): OverlappingBlock[KeyT, ResultT]

  def filter(p: (KeyT, ValueT) => Boolean): OverlappingBlock[KeyT, ValueT]

  def map[ResultT: ClassTag](f: (KeyT, ValueT) => ResultT): OverlappingBlock[KeyT, ResultT]

  def reduce(f: ((KeyT, ValueT), (KeyT, ValueT)) => (KeyT, ValueT)): (KeyT, ValueT)

  def fold(zeroValue: (KeyT, ValueT))(op: ((KeyT, ValueT), (KeyT, ValueT)) => (KeyT, ValueT)): (KeyT, ValueT)

  /*
  Directly fold the results of a kernel operation in order to reduce memory burden.
   */
  def slidingFold[ResultT: ClassTag](size: Array[IntervalSize],
                                     targets: Array[CompleteLocation[KeyT]])
                                    (f: Array[(KeyT, ValueT)] => ResultT,
                                     zero: ResultT,
                                     op: (ResultT, ResultT) => ResultT): ResultT

  /*
  Same thing, the targets being all the admissible points of the block.
   */
  def slidingFold[ResultT: ClassTag](size: Array[IntervalSize])
                                    (f: Array[(KeyT, ValueT)] => ResultT,
                                     zero: ResultT,
                                     op: (ResultT, ResultT) => ResultT): ResultT

  /*
  Compute kernel f on all the windows devised by the array of predicates and fold the results.
   */
  def slicingWindowFold[ResultT: ClassTag](cutPredicates: Array[(KeyT, KeyT) => Boolean])
                                          (f: Array[(KeyT, ValueT)] => ResultT,
                                           zero: ResultT,
                                           op: (ResultT, ResultT) => ResultT): ResultT

  def toArray: Array[(KeyT, ValueT)] // Guarantees there is no redundancy here

  def count: Long

  def take(n: Int): Array[(KeyT, ValueT)]

}

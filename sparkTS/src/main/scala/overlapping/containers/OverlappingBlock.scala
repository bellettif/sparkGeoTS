package main.scala.overlapping.containers

import scala.reflect.ClassTag

/**
 *  Main trait of the main.scala.overlapping weak memory data project.
 *  The distributed representation of data will be based on a key, value RDD of
 *  (int, OverlappingBlock) where the key is the index of the partition.
 */
abstract class OverlappingBlock[KeyT : Ordering, ValueT] extends Serializable{

  /**
   * Raw data.
   *
   * @return Raw representation of data (with padding).
   */
  def data: Array[(KeyT, ValueT)]

  /**
   * Locations that keep track of the current partition index and the original partition index.
   * Redundant elements will have different partition index and original partition index.
   *
   * @return Partition aware representation of keys.
   */
  def locations: Array[CompleteLocation[KeyT]] // Can be evenly spaced or not

  /**
   * Apply the kernel with a window width of size (array of sizes, one per layout axis),
   * centering the kernel computation on the targets.
   *
   * @param selection Selection predicate for the window. (Left argument is kernel target).
   * @param targets Targets the kernel computations will be centered about. If None all admissible point within the block.
   * @param kernel The kernel function that will be applied to each kernel input.
   * @param targetFilter A filter to apply on target timestamps and values.
   * @param windowFilter A filter to appy on window content.
   * @tparam ResultT Kernel return type.
   * @return A time series with the resulting kernel computations. (Padding not guaranteed).
   */
  def sliding[ResultT: ClassTag](
      selection: (KeyT, KeyT) => Boolean,
      targets: Option[Array[CompleteLocation[KeyT]]] = None)
      (kernel: Array[(KeyT, ValueT)] => ResultT,
      targetFilter: Option[KeyT => Boolean] = None,
      windowFilter: Option[Array[(KeyT, ValueT)] => Boolean] = None): OverlappingBlock[KeyT, ResultT]

  /**
   * Roll apply the kernel centering all computations on the targets data points
   * of the overlapping block, keep a state in memory.
   *
   * @param selection Selection predicate for the window. (Left argument is kernel target).
   * @param targets The points about which the quantities should be computed. If None all admissible point within the block.
   * @param kernel The kernel function that will be applied to each kernel input.
   * @param init Initialization of the state.
   * @param targetFilter A filter to apply on target timestamps and values.
   * @param windowFilter A filter to appy on window content.
   * @tparam ResultT Kernel return type.
   * @tparam MemType Memory state type.
   * @return
   */
  def slidingWithMemory[ResultT: ClassTag, MemType: ClassTag](
      selection: (KeyT, KeyT) => Boolean,
      targets: Option[Array[CompleteLocation[KeyT]]] = None)
      (kernel: (Array[(KeyT, ValueT)], MemType) => (ResultT, MemType),
      init: MemType,
      targetFilter: Option[KeyT => Boolean] = None,
      windowFilter: Option[Array[(KeyT, ValueT)] => Boolean] = None): SingleAxisBlock[KeyT, ResultT]

  /**
   * Create a new overlapping block based on filtered values of the current one.
   *
   * @param p Filtering predicate.
   * @return A new filtered overlapping block.
   */
  def filter(p: (KeyT, ValueT) => Boolean): OverlappingBlock[KeyT, ValueT]

  /**
   * Create a new overlapping block based on mapped values of the current one.
   *
   * @param f Function to map on each key/value pair.
   * @tparam ResultT Type of the result
   * @return New overlapping block whose keys are similar but with transformed values.
   */
  def map[ResultT: ClassTag](
      f: (KeyT, ValueT) => ResultT,
      filter: Option[(KeyT, ValueT) => Boolean] = None): OverlappingBlock[KeyT, ResultT]

  /**
   * Reduce the data of the overlapping block.
   *
   * @param f Reduction operator.
   * @return Result of the overall reduction.
   */
  def reduce(
      f: ((KeyT, ValueT), (KeyT, ValueT)) => (KeyT, ValueT),
      filter: Option[(KeyT, ValueT) => Boolean] = None): (KeyT, ValueT)

  /**
   * Fold the overlapping block with the results of a pre-mapped function on each key/value pair.
   *
   * @param zeroValue Neutral element of the reduction operator.
   * @param f Function that will be applied and whose results will be reduced.
   * @param op Reducing operator.
   * @tparam ResultT Type of the result.
   * @return
   */
  def fold[ResultT: ClassTag](zeroValue: ResultT)(
    f: (KeyT, ValueT) => ResultT,
    op: (ResultT, ResultT) => ResultT,
    filter: Option[(KeyT, ValueT) => Boolean] = None): ResultT

  /**
   * Directly fold the results of a kernel operation in order to reduce memory burden.
   *
   * @param selection Selection predicate of the input window to the kernel. (Left argument is kernel target).
   * @param targets Target centers about which the windows will be spanned. If none, all admissible points.
   * @param kernel Kernel function that will be applied to the data in each input window.
   * @param zero Neutral element of the reducing operator.
   * @param op Reducing operator.
   * @param targetFilter Filter predicate on the kernel target.
   * @param windowFilter Filter predicate on the kernel input window.
   * @tparam ResultT Type of the kernel output.
   * @return Reduced output values of the kernel operator applied to the input window.
   */
  def slidingFold[ResultT: ClassTag](
      selection: (KeyT, KeyT) => Boolean,
      targets: Option[Array[CompleteLocation[KeyT]]] = None)
      (kernel: Array[(KeyT, ValueT)] => ResultT,
      zero: ResultT,
      op: (ResultT, ResultT) => ResultT,
      targetFilter: Option[KeyT => Boolean] = None,
      windowFilter: Option[Array[(KeyT, ValueT)] => Boolean] = None): ResultT

  /**
   *  Stateful sliding fold with a memory state. Run about all prescribed targets.
   *
   * @param selection Selection predicate of the input window to the kernel. (Left argument is kernel target).
   * @param targets Target centers about which the windows will be spanned. If none, all admissible points.
   * @param kernel Kernel function that will be applied to the data in each input window.
   * @param zero Neutral element of the reducing operator.
   * @param op Reducing operator.
   * @param init Initial value for the state.
   * @param targetFilter Filter predicate on the kernel target.
   * @param windowFilter Filter predicate on the kernel input window.
   * @tparam ResultT Type of the kernel output.
   * @tparam MemType Type of the memory state.
   * @return The reduced output of all stateful kernels.
   */
  def slidingFoldWithMemory[ResultT: ClassTag, MemType: ClassTag](
      selection: (KeyT, KeyT) => Boolean,
      targets: Option[Array[CompleteLocation[KeyT]]] = None)
      (kernel: (Array[(KeyT, ValueT)], MemType) => (ResultT, MemType),
      zero: ResultT,
      op: (ResultT, ResultT) => ResultT,
      init: MemType,
      targetFilter: Option[KeyT => Boolean] = None,
      windowFilter: Option[Array[(KeyT, ValueT)] => Boolean] = None): ResultT

  /**
   * Return a representation of the overlapping block without redundant data.
   *
   * @return An array instead of the overlapping block.
   */
  def toArray: Array[(KeyT, ValueT)]

  /**
   * Count the number of elements in the overlapping that are not replicas.
   *
   * @return The number of original elements in the overlapping block.
   */
  def count: Long

  /**
   * Get the n first elements in the overlapping block (that are not replicas).
   *
   * @param n Number of elements to take.
   * @return N first original elements in the overlapping block.
   */
  def take(n: Int): Array[(KeyT, ValueT)]

}

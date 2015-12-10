package main.scala.overlapping.containers

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 12/4/15.
 */

/**
 * Time series container encapsulating class.
 *
 * @tparam IndexT Type of time stamp.
 * @tparam ValueT Type of value.
 */
abstract class KernelizedTS[IndexT : TSInstant : ClassTag, ValueT : ClassTag](
    val config: TSConfig[IndexT]){

  def sliding[ResultT: ClassTag](
      selection: (IndexT, IndexT) => Boolean,
      kernel: Array[(IndexT, ValueT)] => ResultT,
      targetFilter: Option[IndexT => Boolean] = None,
      windowFilter: Option[Array[(IndexT, ValueT)] => Boolean] = None): KernelizedTS[IndexT, ResultT]

  def slidingWithMemory[ResultT: ClassTag, MemType: ClassTag](
      selection: (IndexT, IndexT) => Boolean,
      kernel: (Array[(IndexT, ValueT)], MemType) => (ResultT, MemType),
      init: MemType,
      targetFilter: Option[IndexT => Boolean] = None,
      windowFilter: Option[Array[(IndexT, ValueT)] => Boolean] = None): KernelizedTS[IndexT, ResultT]

  def filter(p: (IndexT, ValueT) => Boolean): KernelizedTS[IndexT, ValueT]

  def map[ResultT: ClassTag](
      f: (IndexT, ValueT) => ResultT,
      filter: Option[(IndexT, ValueT) => Boolean] = None): KernelizedTS[IndexT, ResultT]

  def reduce(
      f: ((IndexT, ValueT), (IndexT, ValueT)) => (IndexT, ValueT),
      filter: Option[(IndexT, ValueT) => Boolean] = None): (IndexT, ValueT)

  def fold[ResultT: ClassTag](zeroValue: ResultT)(
      f: (IndexT, ValueT) => ResultT,
      op: (ResultT, ResultT) => ResultT,
      filter: Option[(IndexT, ValueT) => Boolean] = None): ResultT

  def slidingFold[ResultT: ClassTag](
      selection: (IndexT, IndexT) => Boolean,
      kernel: Array[(IndexT, ValueT)] => ResultT,
      zero: ResultT,
      op: (ResultT, ResultT) => ResultT,
      targetFilter: Option[IndexT => Boolean] = None,
      windowFilter: Option[Array[(IndexT, ValueT)] => Boolean] = None): ResultT

  def slidingFoldWithMemory[ResultT: ClassTag, MemType: ClassTag](
      selection: (IndexT, IndexT) => Boolean,
      kernel: (Array[(IndexT, ValueT)], MemType) => (ResultT, MemType),
      zero: ResultT,
      op: (ResultT, ResultT) => ResultT,
      init: MemType,
      targetFilter: Option[IndexT => Boolean] = None,
      windowFilter: Option[Array[(IndexT, ValueT)] => Boolean] = None): ResultT

  def count: Long

  def toArray(sample: Option[Int] = None): Array[(IndexT, ValueT)]


}

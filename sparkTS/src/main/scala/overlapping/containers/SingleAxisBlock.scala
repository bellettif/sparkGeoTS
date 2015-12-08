package main.scala.overlapping.containers

import main.scala.overlapping.CompleteLocation

import scala.reflect.ClassTag


object SingleAxisBlock{

  /**
   * Create an overlapping block based on raw replicated data.
   * The data will be sorted with respect to keys.
   *
   * @param rawData Raw replicated data (unsorted).
   * @tparam IndexT Type of index used for the overlapping block.
   * @tparam ValueT Type of values in the overlapping block.
   * @return A single axis overlapping block.
   */
  def apply[IndexT : TSInstant : ClassTag, ValueT: ClassTag]
    (rawData: Array[((Int, Int, IndexT), ValueT)]): SingleAxisBlock[IndexT, ValueT] ={

    val sortedData = rawData
      .sortBy(_._1._3)

    val data = sortedData
      .map({case (k, v) => (k._3, v)})

    val locations: Array[CompleteLocation[IndexT]] = sortedData
      .map({case (k, v) => CompleteLocation(k._1, k._2, k._3)})

    new SingleAxisBlock[IndexT, ValueT](data, locations)

  }

}


class SingleAxisBlock[IndexT : TSInstant : ClassTag, ValueT: ClassTag](
    val data: Array[(IndexT, ValueT)],
    val locations: Array[CompleteLocation[IndexT]])
  extends OverlappingBlock[IndexT, ValueT]{

  lazy val firstValidIndex = locations.indexWhere(x => x.partIdx == x.originIdx)

  lazy val lastValidIndex  = locations.lastIndexWhere(x => x.partIdx == x.originIdx, locations.length - 1)

  /**
   * Get the index of a kernel input window.
   * @param beginIndex Where to begin the search.
   * @param endIndex Where to end the search.
   * @param t The target around which the search will be conducted.
   * @param selection Window selection predicate.
   * @return Indices of the start and end of the window (inclusive).
   */
  def getWindowIndex(beginIndex: Int, endIndex: Int, t: IndexT, selection: (IndexT, IndexT) => Boolean): (Int, Int) ={

    var beginIndex_ = beginIndex
    var endIndex_ = endIndex

    beginIndex_ = locations.indexWhere(x => selection(t, x.k),
      beginIndex)

    endIndex_ = locations.indexWhere(x => selection(t, x.k),
      endIndex)

    if (endIndex_ == -1) {
      endIndex_ = data.length - 1
    }

    endIndex_ = locations.lastIndexWhere(x => selection(t, x.k),
      endIndex_)

    (beginIndex_, endIndex_)

  }

  /**
   * Apply the kernel to the window.
   * None will also be returned based on the filtering predicated (if any).
   *
   * @param targetIdx Index of the target.
   * @param beginIndex Index where to begin the window (inclusive).
   * @param endIndex Index where to end the window (inclusive).
   * @param kernel Function to apply to the range of data.
   * @param targetFilter Filtering predicate on the timestamp of the target.
   * @param windowFilter Filtering predicate on the value within the window.
   * @tparam ResultT
   * @return None if filtered out, the result otherwise.
   */
  def applyKernel[ResultT: ClassTag](
      targetIdx: IndexT,
      beginIndex: Int,
      endIndex: Int,
      kernel: Array[(IndexT, ValueT)] => ResultT,
      targetFilter: Option[IndexT => Boolean] = None,
      windowFilter: Option[Array[(IndexT, ValueT)] => Boolean] = None): Option[ResultT] = {

    val filterTargets = targetFilter.isDefined
    val filterWindows = windowFilter.isDefined

    if(! filterTargets){
      val targetFilter_ = targetFilter.get
      if(! targetFilter_(targetIdx)){
        return None
      }
    }

    if(! filterWindows){
      val windowFilter_ = windowFilter.get
      if(! windowFilter_(data.slice(beginIndex, endIndex + 1))){
        return None
      }
    }

    Some(kernel(data.slice(beginIndex, endIndex + 1)))

  }

  /**
   * Apply the stateful (with memory) kernel to the window.
   * None will also be returned based on the filtering predicated (if any).
   *
   * @param targetIdx Index of the target.
   * @param beginIndex Index where to begin the window (inclusive).
   * @param endIndex Index where to end the window (inclusive).
   * @param kernel Function to apply to the range of data.
   * @param targetFilter Filtering predicate on the timestamp of the target.
   * @param windowFilter Filtering predicate on the value within the window.
   * @tparam ResultT
   * @return (None if filtered out, the result otherwise, new state).
   */
  def applyMemoryKernel[ResultT: ClassTag, MemType: ClassTag](
      targetIdx: IndexT,
      beginIndex: Int,
      endIndex: Int,
      memState: MemType,
      kernel: (Array[(IndexT, ValueT)], MemType) => (ResultT, MemType),
      targetFilter: Option[IndexT => Boolean] = None,
      windowFilter: Option[Array[(IndexT, ValueT)] => Boolean] = None): (Option[ResultT], MemType) = {

    val filterTargets = targetFilter.isDefined
    val filterWindows = windowFilter.isDefined

    if(! filterTargets){
      val targetFilter_ = targetFilter.get
      if(! targetFilter_(targetIdx)){
        return (None, memState)
      }
    }

    if(! filterWindows){
      val windowFilter_ = windowFilter.get
      if(! windowFilter_(data.slice(beginIndex, endIndex + 1))){
        return (None, memState)
      }
    }

    val (kResult, newMemState) = kernel(data.slice(beginIndex, endIndex + 1), memState)
    (Some(kResult), newMemState)

  }

  /**
   * Implementation of sliding in the case of a temporal leading axis for continuous or irregularly spaced data.
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
      selection: (IndexT, IndexT) => Boolean,
      targets: Option[Array[CompleteLocation[IndexT]]] = None)
      (kernel: Array[(IndexT, ValueT)] => ResultT,
      targetFilter: Option[IndexT => Boolean] = None,
      windowFilter: Option[Array[(IndexT, ValueT)] => Boolean] = None): SingleAxisBlock[IndexT, ResultT] = {

    val targets_ = targets.getOrElse(locations.slice(firstValidIndex, lastValidIndex + 1))

    val result = Array.ofDim[(IndexT, Option[ResultT])](targets_.length)
    var validResults = Array.fill(targets_.length)(false).zipWithIndex

    var i = 0
    var beginIndex = 0
    var endIndex   = 0

    while(i < targets_.length){

      if(endIndex != 1) {

        val centerLocation = targets_(i)

        val (beginIndex_, endIndex_) = getWindowIndex(beginIndex, endIndex, centerLocation.k, selection)
        beginIndex = beginIndex_
        endIndex = endIndex_

        // Check that some data is within range
        if ((beginIndex != -1) && (endIndex != -1)) {
          result(i) = (centerLocation.k, applyKernel(
            targets_(i).k,
            beginIndex,
            endIndex,
            kernel,
            targetFilter,
            windowFilter))
          validResults(i) = (result(i)._2.isDefined, i)
        }

      }

      i += 1
    }

    validResults = validResults.filter(x => x._1)

    new SingleAxisBlock[IndexT, ResultT](
      validResults.map(i => result(i._2)).map({case (x, y) => (x, y.get)}),
      validResults.map(i => targets.get(i._2)))

  }

  /**
   * Implementation of sliding with memory in the case of a temporal leading axis for continuous or irregularly spaced data.
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
  override def slidingWithMemory[ResultT: ClassTag, MemType: ClassTag](
      selection: (IndexT, IndexT) => Boolean,
      targets: Option[Array[CompleteLocation[IndexT]]] = None)
      (kernel: (Array[(IndexT, ValueT)], MemType) => (ResultT, MemType),
      init: MemType,
      targetFilter: Option[IndexT => Boolean] = None,
      windowFilter: Option[Array[(IndexT, ValueT)] => Boolean] = None): SingleAxisBlock[IndexT, ResultT] = {

    val targets_ = targets.getOrElse(locations.slice(firstValidIndex, lastValidIndex + 1))

    val result = Array.ofDim[(IndexT, Option[ResultT])](targets_.length)
    var validResults = Array.fill(targets_.length)(false).zipWithIndex

    var i = 0
    var beginIndex = 0
    var endIndex   = 0

    var memState = init

    while(i < targets_.length){

      if(endIndex != 1) {

        val centerLocation = targets_(i)

        val (beginIndex_, endIndex_) = getWindowIndex(beginIndex, endIndex, centerLocation.k, selection)
        beginIndex = beginIndex_
        endIndex = endIndex_

        // Check that some data is within range
        if ((beginIndex != -1) && (endIndex != -1)) {
          val (kResult, newMemState) = applyMemoryKernel(
            targets_(i).k,
            beginIndex,
            endIndex,
            memState,
            kernel,
            targetFilter,
            windowFilter)

          result(i) = (centerLocation.k, kResult)
          memState = newMemState

          validResults(i) = (kResult.isDefined, i)
        }

      }

      i += 1
    }

    validResults = validResults.filter(x => x._1)

    new SingleAxisBlock[IndexT, ResultT](
      validResults.map(i => result(i._2)).map({case (x, y) => (x, y.get)}),
      validResults.map(i => targets.get(i._2)))

  }

  /**
   * Create a new overlapping block based on filtered values of the current one.
   *
   * @param p Filtering predicate.
   * @return A new filtered overlapping block.
   */
  override def filter(p: (IndexT, ValueT) => Boolean): SingleAxisBlock[IndexT, ValueT] = {

    val p_ = p.tupled

    val validIndices = data.indices.filter(i => p_(data(i))).toArray

    new SingleAxisBlock[IndexT, ValueT](
      validIndices.map(i => data(i)),
      validIndices.map(i => locations(i)))

  }

  /**
   * Create a new overlapping block based on mapped values of the current one.
   *
   * @param f Function to map on each key/value pair.
   * @param filter
   * @tparam ResultT Type of the result
   * @return New overlapping block whose keys are similar but with transformed values.
   */
  override def map[ResultT: ClassTag](
      f: (IndexT, ValueT) => ResultT,
      filter: Option[(IndexT, ValueT) => Boolean] = None): SingleAxisBlock[IndexT, ResultT] = {

    if(filter.isDefined) {

      val filter_ = filter.get.tupled
      val validIndices = data.indices.filter(i => filter_(data(i))).toArray
      new SingleAxisBlock[IndexT, ResultT](validIndices.map({ case i => (data(i)._1, f(data(i)._1, data(i)._2)) }), locations)

    }else{

      new SingleAxisBlock[IndexT, ResultT](data.map({ case (k, v) => (k, f(k, v)) }), locations)

    }

  }

  /**
   * Reduce the data of the overlapping block.
   *
   * @param f Reduction operator.
   * @return Result of the overall reduction.
   */
  override def reduce(
      f: ((IndexT, ValueT), (IndexT, ValueT)) => (IndexT, ValueT),
      filter: Option[(IndexT, ValueT) => Boolean] = None): (IndexT, ValueT) = {

    if(filter.isEmpty) {
      data.slice(firstValidIndex, lastValidIndex + 1).reduce(f)
    }else{
      val filter_ = filter.get.tupled
      data.slice(firstValidIndex, lastValidIndex + 1).filter(filter_).reduce(f)
    }

  }

  /**
   * Fold the overlapping block with the results of a pre-mapped function on each key/value pair.
   *
   * @param zeroValue Neutral element of the reduction operator.
   * @param f Function that will be applied and whose results will be reduced.
   * @param op Reducing operator.
   * @param filter
   * @tparam ResultT Type of the result.
   * @return
   */
  override def fold[ResultT: ClassTag](zeroValue: ResultT)(
      f: (IndexT, ValueT) => ResultT,
      op: (ResultT, ResultT) => ResultT,
      filter: Option[(IndexT, ValueT) => Boolean] = None): ResultT = {

    var result = zeroValue

    var i = firstValidIndex

    if(filter.isEmpty) {

      while (i <= lastValidIndex) {
        result = op(result, f(data(i)._1, data(i)._2))
        i += 1
      }

    }else{

      val filter_ = filter.get.tupled

      while (i <= lastValidIndex) {

        if(filter_(data(i))) {
          result = op(result, f(data(i)._1, data(i)._2))
        }

        i += 1
      }

    }

    result

  }

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
  override def slidingFold[ResultT: ClassTag](
      selection: (IndexT, IndexT) => Boolean,
      targets: Option[Array[CompleteLocation[IndexT]]] = None)
      (kernel: Array[(IndexT, ValueT)] => ResultT,
      zero: ResultT,
      op: (ResultT, ResultT) => ResultT,
      targetFilter: Option[IndexT => Boolean] = None,
      windowFilter: Option[Array[(IndexT, ValueT)] => Boolean] = None): ResultT = {

    val targets_ = targets.getOrElse(locations.slice(firstValidIndex, lastValidIndex + 1))

    var result = zero
    var i = 0
    var beginIndex = 0
    var endIndex   = 0

    while(i < targets_.length){

      if(endIndex != 1) {

        val centerLocation = targets_(i)

        val (beginIndex_, endIndex_) = getWindowIndex(beginIndex, endIndex, centerLocation.k, selection)
        beginIndex = beginIndex_
        endIndex = endIndex_

        // Check that some data is within range
        if ((beginIndex != -1) && (endIndex != -1)) {

          result = op(result, applyKernel(
            targets_(i).k,
            beginIndex,
            endIndex,
            kernel,
            targetFilter,
            windowFilter).getOrElse(zero))

        }

      }

      i += 1

    }

    result

  }


  def slidingFoldWithMemory[ResultT: ClassTag, MemType: ClassTag](
      selection: (IndexT, IndexT) => Boolean,
      targets: Option[Array[CompleteLocation[IndexT]]] = None)
      (kernel: (Array[(IndexT, ValueT)], MemType) => (ResultT, MemType),
      zero: ResultT,
      op: (ResultT, ResultT) => ResultT,
      init: MemType,
      targetFilter: Option[IndexT => Boolean] = None,
      windowFilter: Option[Array[(IndexT, ValueT)] => Boolean] = None): ResultT = {

    val targets_ = targets.getOrElse(locations.slice(firstValidIndex, lastValidIndex + 1))

    var result = zero

    var i = 0
    var beginIndex = 0
    var endIndex   = 0

    var memState = init

    while(i < targets_.length){

      if(endIndex != 1) {

        val centerLocation = targets_(i)

        val (beginIndex_, endIndex_) = getWindowIndex(beginIndex, endIndex, centerLocation.k, selection)
        beginIndex = beginIndex_
        endIndex = endIndex_

        // Check that some data is within range
        if ((beginIndex != -1) && (endIndex != -1)) {
          val (kResult, newMemState) = applyMemoryKernel(
            targets_(i).k,
            beginIndex,
            endIndex,
            memState,
            kernel,
            targetFilter,
            windowFilter)

          result = op(result, kResult.getOrElse(zero))
          memState = newMemState

        }

      }

      i += 1
    }

    result

  }

  override def toArray: Array[(IndexT, ValueT)] = {

    data.slice(firstValidIndex, lastValidIndex + 1)

  }

  override def count: Long = {

    lastValidIndex + 1 - firstValidIndex

  }

  override def take(n : Int): Array[(IndexT, ValueT)] = {

    data.slice(firstValidIndex, lastValidIndex + 1).take(n)

  }

}

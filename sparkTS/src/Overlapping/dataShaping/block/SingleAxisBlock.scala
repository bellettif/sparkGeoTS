package overlapping.dataShaping.block

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 8/7/15.
 */
class SingleAxisBlock[IndexT <: Ordered[IndexT], ValueT: ClassTag](
    val rawData: Array[((Int, Int, IndexT), ValueT)])
  extends OverlappingBlock[IndexT, ValueT]{

  val sortedData: Array[((Int, Int, IndexT), ValueT)] = rawData
    .sortBy(_._1._3)

  val data: Array[ValueT] = sortedData
    .map({case (k, v) => v})

  val locations: Array[CompleteLocation] = sortedData
    .map({case (k, v) => CompleteLocation(k._1, k._2, k._3)})

  lazy val firstValidIndex = locations.indexWhere(x => x.partIdx == x.originIdx)

  lazy val lastValidIndex = locations.lastIndexWhere(x => x.partIdx == x.originIdx)


  override def sliding(size: Array[KernelSize], stride: Array[Int]): Unit ={

    val activeKernelSize    = size.head
    val activeStride        = stride.head

    val lookBack  = activeKernelSize.lookBack
    val lookAhead = activeKernelSize.lookAhead
    val totSize   = lookBack + lookAhead + 1

    data
      .slice(firstValidIndex - lookBack, lastValidIndex + lookAhead)
      .sliding(totSize, activeStride)

  }


  override def toIterator(): Iterator[ValueT] ={

    data
      .slice(firstValidIndex, lastValidIndex)
      .iterator

  }


  // Define the padding of the iterator in terms of T along each direction
  override def toIterator(offsets: Array[Double]): Iterator[ValueT] = {

    val activeAlgebraicDistance   = algebraicDistances.head
    val activeOffset              = offsets.head
    val firstValidDatum           = data.apply(firstValidIndex)

    data
      .drop(firstValidIndex)
      .dropWhile(activeAlgebraicDistance(firstValidDatum, _) >= activeOffset)
      .toIterator

  }

  // Define the padding in terms of number of records along each direction
  override def toIterator(offsets: Array[Int]): Iterator[ValueT] = {

    val activeAlgebraicDistance   = algebraicDistances.head
    val activeOffset              = offsets.head
    val firstValidDatum           = data.apply(firstValidIndex)

    data
      .drop(firstValidIndex)
      .drop(activeOffset)
      .toIterator

  }



}

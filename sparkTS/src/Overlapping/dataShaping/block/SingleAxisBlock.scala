package overlapping.dataShaping.block

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 8/7/15.
 */
class SingleAxisBlock[IndexT <: Ordered[IndexT], ValueT: ClassTag](
    val rawData: Array[((Int, Int, IndexT), ValueT)],
    val algDistanaces: Array[(ValueT, ValueT) => Double])
  extends OverlappingBlock[IndexT, ValueT]{

  val sortedData: Array[((Int, Int, IndexT), ValueT)] = rawData
    .sortBy(_._1._3)

  val data: Array[ValueT] = sortedData
    .map({case (k, v) => v})

  val locations: Array[CompleteLocation] = sortedData
    .map({case (k, v) => CompleteLocation(k._1, k._2, k._3)})

  lazy val firstValidIndex = locations.indexWhere(x => x.partIdx == x.originIdx)
  lazy val lastValidIndex  = locations.lastIndexWhere(x => x.partIdx == x.originIdx)


  override def sliding(size: Array[KernelSize], stride: Array[Int]): Iterator[Array[ValueT]] ={

    // Here there is only one dimension thus one kernel and one kernel size
    val activeKernelSize    = size.head
    val activeStride        = stride.head

    val lookBack  = activeKernelSize.lookBack
    val lookAhead = activeKernelSize.lookAhead
    val totSize   = lookBack + lookAhead + 1

    data
      .slice(firstValidIndex - lookBack, lastValidIndex + lookAhead)
      .sliding(totSize, activeStride)

  }

  def sliding(size: Array[IntervalSize]): Iterator[Array[(IndexT, ValueT)]] = {

    val activeIntervalSize = size.head

    val lookBack  = activeIntervalSize.lookBack
    val lookAhead = activeIntervalSize.lookAhead

    val activeDistance = algebraicDistances.head

    val buff = locations.toIterator.zip(data.toIterator)

    var preBuff = List[(IndexT, ValueT)]()
    var postBuff = List[(IndexT, ValueT)]()

    var result = List[Array[(IndexT, ValueT)]]()

    def scanAhead() = {

    }

    def dropBack() = {

    }

    while(buff.hasNext){
      val (loc: CompleteLocation, v: ValueT) = buff.next()

      result = (preBuff ::: ((loc.k, v) :: postBuff)).toArray :: result
    }

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

  // Can be evenly spaced or not
  override def algebraicDistances: Array[(ValueT, ValueT) => Double] = algDistanaces

}

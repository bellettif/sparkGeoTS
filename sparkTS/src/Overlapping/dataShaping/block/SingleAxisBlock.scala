package overlapping.dataShaping.block

import overlapping.CompleteLocation

import scala.reflect.ClassTag

object SingleAxisBlock{

  def apply[IndexT <: Ordered[IndexT], ValueT: ClassTag]
    (rawData: Array[((Int, Int, IndexT), ValueT)],
    signedDistances: Array[(IndexT, IndexT) => Double]): SingleAxisBlock[IndexT, ValueT] ={

    val sortedData: Array[((Int, Int, IndexT), ValueT)] = rawData
      .sortBy(_._1._3)

    val data: Array[(IndexT, ValueT)] = sortedData
      .map({case (k, v) => (k._3, v)})

    val locations: Array[CompleteLocation[IndexT]] = sortedData
      .map({case (k, v) => CompleteLocation(k._1, k._2, k._3)})

    new SingleAxisBlock[IndexT, ValueT](data, locations, signedDistances)

  }

}


/**
 * Created by Francois Belletti on 8/7/15.
 */
class SingleAxisBlock[IndexT <: Ordered[IndexT], ValueT: ClassTag](
    val data: Array[(IndexT, ValueT)],
    val locations: Array[CompleteLocation[IndexT]],
    val signedDistances: Array[(IndexT, IndexT) => Double])
  extends OverlappingBlock[IndexT, ValueT]{



  lazy val firstValidIndex = locations.indexWhere(x => x.partIdx == x.originIdx)
  lazy val lastValidIndex  = locations.lastIndexWhere(x => x.partIdx == x.originIdx)

  val signedDistance = signedDistances.apply(0)


  override def sliding(size: Array[IntervalSize]): OverlappingBlock[IndexT, Array[(IndexT, ValueT)]] = {

    sliding(size, locations.slice(firstValidIndex, lastValidIndex + 1).toIterator)

  }


  override def sliding(size: Array[IntervalSize], targets: Iterator[CompleteLocation[IndexT]]): OverlappingBlock[IndexT, Array[(IndexT, ValueT)]] = {

    val lookAhead = size.head.lookAhead
    val lookBack  = size.head.lookBack

    var begin_index = 0
    var end_index   = firstValidIndex

    var result = List[(IndexT, Array[(IndexT, ValueT)])]()

    for(center_location <- targets){
      begin_index = locations.indexWhere(x => signedDistance(x.k, center_location.k) <= lookBack,
        begin_index)

      end_index = locations.lastIndexWhere(x => signedDistance(center_location.k, x.k) <= lookAhead,
        end_index)

      result = result :+ (center_location.k, data.slice(begin_index, end_index + 1))

    }

    new SingleAxisBlock[IndexT, Array[(IndexT, ValueT)]](result.toArray, targets.toArray, signedDistances)

  }


  override def filter(p: (IndexT, ValueT) => Boolean)(size: Array[IntervalSize]): SingleAxisBlock[IndexT, ValueT] = {

    val p_ = p.tupled

    val validIndices = data.indices.filter(i => p_(data.apply(i))).toArray

    new SingleAxisBlock[IndexT, ValueT](
      validIndices.map(i => data.apply(i)),
      validIndices.map(i => locations.apply(i)),
      signedDistances)

  }

  def map[ResultT](f: (IndexT, ValueT) => ResultT): SingleAxisBlock[IndexT, ResultT] = {

    new SingleAxisBlock[IndexT, ResultT](data.map({case (k, v) => (k, f(k, v))}), locations, signedDistances)

  }

  def reduce(f: (ValueT, ValueT) => ValueT): ValueT = {

    data.slice(firstValidIndex, lastValidIndex + 1).map(_._2).reduce(f)

  }

  def fold(zeroValue: ValueT)(op: (ValueT, ValueT) => ValueT): ValueT = {

    data.slice(firstValidIndex, lastValidIndex + 1).map(_._2).fold(zeroValue)(op)

  }

  override def toIterator: Iterator[(IndexT, ValueT)] = {

    data.slice(firstValidIndex, lastValidIndex + 1).toIterator

  }

}

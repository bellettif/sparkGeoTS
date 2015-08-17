package overlapping.containers.block

import overlapping.CompleteLocation

import scala.reflect.ClassTag

object SingleAxisBlock{

  def apply[IndexT <: Ordered[IndexT], ValueT: ClassTag]
    (rawData: Array[((Int, Int, IndexT), ValueT)],
    signedDistance: (IndexT, IndexT) => Double): SingleAxisBlock[IndexT, ValueT] ={

    val sortedData: Array[((Int, Int, IndexT), ValueT)] = rawData
      .sortBy(_._1._3)

    val data: Array[(IndexT, ValueT)] = sortedData
      .map({case (k, v) => (k._3, v)})

    val locations: Array[CompleteLocation[IndexT]] = sortedData
      .map({case (k, v) => CompleteLocation(k._1, k._2, k._3)})

    val signedDistances: Array[((IndexT, IndexT) => Double)] = Array(signedDistance)

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
  lazy val lastValidIndex  = locations.lastIndexWhere(x => x.partIdx == x.originIdx, locations.length - 1)

  val signedDistance = signedDistances.apply(0)


  override def sliding(size: Array[IntervalSize]): OverlappingBlock[IndexT, Array[(IndexT, ValueT)]] = {

    sliding(size, locations.slice(firstValidIndex, lastValidIndex + 1).toIterator)

  }


  override def sliding(size: Array[IntervalSize], targets: Iterator[CompleteLocation[IndexT]]): OverlappingBlock[IndexT, Array[(IndexT, ValueT)]] = {

    val lookAhead = size.head.lookAhead
    val lookBack  = size.head.lookBack

    var begin_index = 0
    var end_index   = 0

    var result = List[(IndexT, Array[(IndexT, ValueT)])]()

    for(center_location <- targets){
      if(end_index != -1) {

        begin_index = locations.indexWhere(x => signedDistance(x.k, center_location.k) <= lookBack,
          begin_index)

        end_index = locations.indexWhere(x => signedDistance(center_location.k, x.k) >= lookAhead,
          end_index)

        if ((begin_index != -1) && (end_index != -1))
          result = result :+(center_location.k, data.slice(begin_index, end_index + 1))

      }
    }

    new SingleAxisBlock[IndexT, Array[(IndexT, ValueT)]](result.toArray, targets.toArray, signedDistances)

  }


  override def slidingWindow(cutPredicates: Array[(IndexT, IndexT) => Boolean]): OverlappingBlock[IndexT, Array[(IndexT, ValueT)]] ={

    val cutPredicate = cutPredicates.head

    var begin_index = 0
    var end_index   = 0

    var resultLocations = List[CompleteLocation[IndexT]]()
    var resultData      = List[(IndexT, Array[(IndexT, ValueT)])]()

    val intervals = locations.zip(locations.drop(1))

    while(end_index < intervals.length){
      end_index = intervals.indexWhere({case (x, y) => cutPredicate(x.k, y.k)}, begin_index)

      val start = locations(begin_index)
      val stop  = locations(end_index)

      if(start.originIdx == start.partIdx){
        resultLocations = resultLocations :+ CompleteLocation[IndexT](start.partIdx, start.originIdx, start.k)
      }else if(stop.originIdx == stop.partIdx){
        resultLocations = resultLocations :+ CompleteLocation[IndexT](stop.partIdx, stop.originIdx, start.k)
      }else{
        resultLocations = resultLocations :+ CompleteLocation[IndexT](start.partIdx, start.originIdx, start.k)
      }

      resultData      = resultData :+ (start.k, data.slice(begin_index, end_index))

      begin_index = end_index + 1
    }

    new SingleAxisBlock[IndexT, Array[(IndexT, ValueT)]](resultData.toArray, resultLocations.toArray, signedDistances)

  }


  override def filter(p: (IndexT, ValueT) => Boolean)(size: Array[IntervalSize]): SingleAxisBlock[IndexT, ValueT] = {

    val p_ = p.tupled

    val validIndices = data.indices.filter(i => p_(data(i))).toArray

    new SingleAxisBlock[IndexT, ValueT](
      validIndices.map(i => data(i)),
      validIndices.map(i => locations(i)),
      signedDistances)

  }

  def map[ResultT: ClassTag](f: (IndexT, ValueT) => ResultT): SingleAxisBlock[IndexT, ResultT] = {

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

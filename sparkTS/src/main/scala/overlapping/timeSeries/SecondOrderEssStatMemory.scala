package main.scala.overlapping.timeSeries

import main.scala.overlapping.containers.{TSInstant, TimeSeries}
import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/10/15.
 */
abstract class SecondOrderEssStatMemory[IndexT <: TSInstant[IndexT], ValueT, ResultT: ClassTag, StateT: ClassTag]
  extends Serializable{

  def selection: (IndexT, IndexT) => Boolean

  def modelOrder: ModelSize

  def modelWidth = modelOrder.lookAhead + modelOrder.lookBack + 1

  def zero: ResultT

  def init: StateT

  def kernel(slice: Array[(IndexT, ValueT)], state: StateT): (ResultT, StateT)

  def reducer(r1: ResultT, r2: ResultT): ResultT

  def timeSeriesStats(timeSeries: TimeSeries[IndexT, ValueT]): ResultT = {

    timeSeries.content
      .mapValues(_.slidingFoldWithMemory(selection)(kernel, zero, reducer, init))
      .map(_._2)
      .fold(zero)(reducer)

  }


}
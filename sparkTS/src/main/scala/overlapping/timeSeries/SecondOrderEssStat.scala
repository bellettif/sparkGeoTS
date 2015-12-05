package main.scala.overlapping.timeSeries

import main.scala.overlapping.containers.{TSInstant, TimeSeries}
import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/10/15.
 */
abstract class SecondOrderEssStat[IndexT <: TSInstant[IndexT], ValueT, ResultT: ClassTag]
  extends Serializable{

  def selection: (IndexT, IndexT) => Boolean

  def modelOrder: ModelSize

  def modelWidth = modelOrder.lookAhead + modelOrder.lookBack + 1

  def zero: ResultT

  def kernel(slice: Array[(IndexT, ValueT)]): ResultT

  def reducer(r1: ResultT, r2: ResultT): ResultT

  def timeSeriesStats(timeSeries: TimeSeries[IndexT, ValueT]): ResultT = {

    timeSeries.content
      .mapValues(_.slidingFold(selection)(kernel, zero, reducer))
      .map(_._2)
      .fold(zero)(reducer)

  }


}
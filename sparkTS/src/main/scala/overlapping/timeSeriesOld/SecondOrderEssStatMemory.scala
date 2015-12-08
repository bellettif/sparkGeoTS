package main.scala.overlapping.timeSeriesOld

import breeze.linalg.DenseVector
import main.scala.overlapping.containers.TSInstant

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/10/15.
 */
abstract class SecondOrderEssStatMemory[IndexT : ClassTag, ResultT: ClassTag, StateT: ClassTag]
  extends Serializable{

  def selection: (TSInstant[IndexT], TSInstant[IndexT]) => Boolean

  def modelOrder: ModelSize

  def modelWidth = modelOrder.lookAhead + modelOrder.lookBack + 1

  def zero: ResultT

  def init: StateT

  def kernel(slice: Array[(TSInstant[IndexT], DenseVector[Double])], state: StateT): (ResultT, StateT)

  def reducer(r1: ResultT, r2: ResultT): ResultT

  def timeSeriesStats(timeSeries: VectTimeSeries[IndexT]): ResultT = {

    timeSeries.content
      .mapValues(_.slidingFoldWithMemory(selection)(kernel, zero, reducer, init))
      .map(_._2)
      .fold(zero)(reducer)

  }


}
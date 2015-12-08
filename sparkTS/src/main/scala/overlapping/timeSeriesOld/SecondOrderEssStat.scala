package main.scala.overlapping.timeSeriesOld

import breeze.linalg.DenseVector
import main.scala.overlapping.containers.TSInstant

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/10/15.
 */
abstract class SecondOrderEssStat[IndexT : ClassTag, ResultT : ClassTag]
  extends Serializable{

  def selection: (TSInstant[IndexT], TSInstant[IndexT]) => Boolean

  def modelOrder: ModelSize

  def modelWidth = modelOrder.lookAhead + modelOrder.lookBack + 1

  def zero: ResultT

  def kernel(slice: Array[(TSInstant[IndexT], DenseVector[Double])]): ResultT

  def reducer(r1: ResultT, r2: ResultT): ResultT

  def timeSeriesStats(timeSeries: VectTimeSeries[IndexT]): ResultT = {

    timeSeries.content
      .mapValues(_.slidingFold(selection)(kernel, zero, reducer))
      .map(_._2)
      .fold(zero)(reducer)

  }


}
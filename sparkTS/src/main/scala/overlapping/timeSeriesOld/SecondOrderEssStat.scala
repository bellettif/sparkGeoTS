package main.scala.overlapping.timeSeriesOld

import main.scala.overlapping.containers.{KernelizedTS, TSInstant}

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/10/15.
 */
abstract class SecondOrderEssStat[IndexT : TSInstant : ClassTag, ValueT : ClassTag, ResultT : ClassTag]
  extends Serializable{

  def selection: (IndexT, IndexT) => Boolean

  def zero: ResultT

  def kernel(slice: Array[(IndexT, ValueT)]): ResultT

  def reducer(r1: ResultT, r2: ResultT): ResultT

  def timeSeriesStats(timeSeries: KernelizedTS[IndexT, ValueT]): ResultT = timeSeries.slidingFold(selection, kernel, zero, reducer)


}
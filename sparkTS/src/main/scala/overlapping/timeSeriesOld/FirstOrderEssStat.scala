package main.scala.overlapping.timeSeriesOld

import breeze.linalg.DenseVector
import main.scala.overlapping.containers.TSInstant

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/10/15.
 */
abstract class FirstOrderEssStat[IndexT : ClassTag, ResultT: ClassTag]
  extends Serializable{

  def zero: ResultT

  def kernel(t: TSInstant[IndexT], v: DenseVector[Double]): ResultT

  def reducer(r1: ResultT, r2: ResultT): ResultT

  def timeSeriesStats(timeSeries: VectTimeSeries[IndexT]): ResultT = {

    timeSeries.content
      .mapValues(_.fold(zero)(kernel, reducer))
      .map(_._2)
      .fold(zero)(reducer)

  }


}
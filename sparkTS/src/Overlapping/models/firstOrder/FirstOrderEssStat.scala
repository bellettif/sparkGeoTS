package overlapping.models.firstOrder

import org.apache.spark.rdd.RDD
import overlapping.containers.block.SingleAxisBlock

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/10/15.
 */
abstract class FirstOrderEssStat[IndexT <: Ordered[IndexT], ValueT, ResultT: ClassTag]
  extends Serializable{

  def zero: ResultT

  def kernel(datum: (IndexT, ValueT)): ResultT = ???

  def reducer(r1: ResultT, r2: ResultT): ResultT = ???

  def windowStats(window: Array[(IndexT, ValueT)]): ResultT = {

    window
      .map(kernel)
      .reduce(reducer)

  }

  def blockStats(block: SingleAxisBlock[IndexT, ValueT]): ResultT = {

    block
      .fold(zero)({case (x, y) => kernel(x, y)}, reducer)

  }

  def timeSeriesStats(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, ValueT])]): ResultT = {

    timeSeries
      .mapValues(blockStats)
      .map(_._2)
      .fold(zero)(reducer)

  }


}
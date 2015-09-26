package overlapping.models.secondOrder

import org.apache.spark.rdd.RDD
import overlapping.IntervalSize
import overlapping.containers.block.SingleAxisBlock
import overlapping.models.{Predictor, Estimator}

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/10/15.
 */
abstract class SecondOrderEssStat[IndexT <: Ordered[IndexT], ValueT, ResultT: ClassTag]
  extends Serializable{

  def kernelWidth: IntervalSize

  def modelOrder: ModelSize

  def modelWidth = modelOrder.lookAhead + modelOrder.lookBack + 1

  def zero: ResultT

  def kernel(slice: Array[(IndexT, ValueT)]): ResultT = ???

  def reducer(r1: ResultT, r2: ResultT): ResultT = ???

  def windowStats(window: Array[(IndexT, ValueT)]): ResultT = {

    window
      .sliding(modelWidth)
      .map(kernel)
      .reduce(reducer)

  }

  def blockStats(block: SingleAxisBlock[IndexT, ValueT]): ResultT = {

    block
      .slidingFold(Array(kernelWidth))(kernel, zero, reducer)

  }

  def timeSeriesStats(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, ValueT])]): ResultT = {

    timeSeries
      .mapValues(blockStats)
      .map(_._2)
      .fold(zero)(reducer)

  }


}
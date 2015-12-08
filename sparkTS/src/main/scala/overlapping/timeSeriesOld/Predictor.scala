package main.scala.overlapping.timeSeriesOld

import breeze.linalg.DenseVector
import main.scala.overlapping.containers.TSInstant

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 9/24/15.
 */
abstract class Predictor[IndexT : ClassTag]
  extends Serializable{

  def selection: (TSInstant[IndexT], TSInstant[IndexT]) => Boolean

  def predictKernel(data: Array[(TSInstant[IndexT], DenseVector[Double])]): DenseVector[Double]

  def residualKernel(data: Array[(TSInstant[IndexT], DenseVector[Double])]): DenseVector[Double] = {
    data(data.length - 1)._2 - predictKernel(data)
  }

  def estimateResiduals(timeSeries: VectTimeSeries[IndexT]): VectTimeSeries[IndexT] = {

    new VectTimeSeries(timeSeries.content
      .mapValues(_.sliding(selection)(residualKernel)),
      timeSeries.config)

  }


}

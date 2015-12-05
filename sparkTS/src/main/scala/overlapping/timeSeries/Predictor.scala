package main.scala.overlapping.timeSeries

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import main.scala.overlapping._
import main.scala.overlapping.containers.{TimeSeries, SingleAxisBlock}

/**
 * Created by Francois Belletti on 9/24/15.
 */
trait Predictor[IndexT <: Ordered[IndexT]]
  extends Serializable{

  def selection: (IndexT, IndexT) => Boolean

  def predictKernel(data: Array[(IndexT, DenseVector[Double])]): DenseVector[Double]

  def residualKernel(data: Array[(IndexT, DenseVector[Double])]): DenseVector[Double] = {
    data(data.length - 1)._2 - predictKernel(data)
  }

  def estimateResiduals(
      timeSeries: TimeSeries[IndexT, DenseVector[Double]]): TimeSeries[IndexT, DenseVector[Double]] = {

    timeSeries.content
      .mapValues(_.sliding(selection)(residualKernel))

  }


}

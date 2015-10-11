package overlapping.timeSeries

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import overlapping._
import overlapping.containers.SingleAxisBlock

/**
 * Created by Francois Belletti on 9/24/15.
 */
trait Predictor[IndexT <: Ordered[IndexT]]
  extends Serializable{

  def size: Array[IntervalSize]

  def predictKernel(data: Array[(IndexT, DenseVector[Double])]): DenseVector[Double] = ???

  def residualKernel(data: Array[(IndexT, DenseVector[Double])]): DenseVector[Double] = {
    data(data.length - 1)._2 - predictKernel(data)
  }

  def predictBlock(block: SingleAxisBlock[IndexT, DenseVector[Double]]):
    SingleAxisBlock[IndexT, DenseVector[Double]] = {

    block.sliding(size)(predictKernel)

  }

  def residualBlock(block: SingleAxisBlock[IndexT, DenseVector[Double]]):
  SingleAxisBlock[IndexT, DenseVector[Double]] = {

    block.sliding(size)(residualKernel)

  }

  def predictAll(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]):
    RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])] = {

    timeSeries
      .mapValues(predictBlock)

  }

  def residualAll(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]):
  RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])] = {

    timeSeries
      .mapValues(residualBlock)

  }


}

package overlapping.models

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import overlapping.IntervalSize
import overlapping.containers.block.SingleAxisBlock

/**
 * Created by Francois Belletti on 9/24/15.
 */
trait Predictor[IndexT <: Ordered[IndexT]]
  extends Serializable{

  def size: Array[IntervalSize]

  def predictKernel(data: Array[(IndexT, DenseVector[Double])]): DenseVector[Double] = ???

  def predictBlock(block: SingleAxisBlock[IndexT, DenseVector[Double]]):
    Array[(IndexT, (DenseVector[Double], DenseVector[Double]))] = {

    val predictions = block.sliding(size)(predictKernel).toArray
    block.toArray.zip(predictions)
      .filter({case ((k1, _), (k2, _)) => k1 == k2})
      .map({case ((k1, v1), (k2, v2)) => (k1, (v1, v1 - v2))})

  }

  def predictAll(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]):
    RDD[(IndexT, (DenseVector[Double], DenseVector[Double]))] = {

    timeSeries
      .flatMap({case (k, b) => predictBlock(b)})

  }

}

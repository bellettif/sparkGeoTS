package overlapping.models.firstOrder

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import overlapping.IntervalSize
import overlapping.containers.block.SingleAxisBlock

/**
 * Created by Francois Belletti on 9/23/15.
 */
class MeanEstimator[IndexT <: Ordered[IndexT]]
  extends FirstOrderModel[IndexT, DenseVector[Double]]{

  def computeCrossCov(slice: Array[(IndexT, DenseVector[Double])]): (DenseVector[Double], Long) = {

    val result = slice(0)._2

    (result, 1L)
  }

  def sumArrays(x: (DenseVector[Double], Long), y: (DenseVector[Double], Long)): (DenseVector[Double], Long) ={
    (x._1 + y._1, x._2 + y._2)
  }

  def normalize(result: (DenseVector[Double], Long)): DenseVector[Double] = {
    result._1 / result._2.toDouble
  }

  def computeSum(timeSeries: SingleAxisBlock[IndexT, DenseVector[Double]]): (DenseVector[Double], Long) ={

    val nCols = timeSeries.take(1)(0)._2.size

    timeSeries
      .fold((DenseVector.zeros[Double](nCols), 0L))({case (t, x) => (x, 1L)}, sumArrays)

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]):
  DenseVector[Double] = {

    normalize(
      timeSeries
      .mapValues(computeSum)
      .map(_._2)
      .reduce(sumArrays)

    )

  }

}

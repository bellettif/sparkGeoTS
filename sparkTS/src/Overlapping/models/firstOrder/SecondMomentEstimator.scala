package overlapping.models.firstOrder

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import overlapping.IntervalSize
import overlapping.containers.block.SingleAxisBlock

/**
 * Created by Francois Belletti on 9/23/15.
 */
class SecondMomentEstimator[IndexT <: Ordered[IndexT]]
  extends FirstOrderModel[IndexT, DenseVector[Double]]{

  def sumArrays(x: (DenseMatrix[Double], Long), y: (DenseMatrix[Double], Long)): (DenseMatrix[Double], Long) ={
    (x._1 + y._1, x._2 + y._2)
  }

  def normalize(result: (DenseMatrix[Double], Long)): DenseMatrix[Double] = {
    result._1 / result._2.toDouble
  }

  def computeSqSum(timeSeries: SingleAxisBlock[IndexT, DenseVector[Double]]): (DenseMatrix[Double], Long) ={

    val nCols = timeSeries.take(1)(0)._2.size

    timeSeries
      .slidingFold(Array(IntervalSize(0, 0)))(
        x => (x(0)._2 * x(0)._2.t, 1L),
        (DenseMatrix.zeros[Double](nCols, nCols), 0L),
        sumArrays)

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]):
  DenseMatrix[Double] = {

    normalize(
      timeSeries
      .mapValues(computeSqSum)
      .map(_._2)
      .reduce(sumArrays)

    )

  }

}

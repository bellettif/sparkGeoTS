package overlapping.models.firstOrder

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import overlapping.IntervalSize
import overlapping.containers.block.SingleAxisBlock
import overlapping.models.Estimator

/**
 * Created by Francois Belletti on 9/23/15.
 */
class SecondMomentEstimator[IndexT <: Ordered[IndexT]](val d: Int)
  extends FirstOrderEssStat[IndexT, DenseVector[Double], (DenseMatrix[Double], Long)]
  with Estimator[IndexT, DenseVector[Double], DenseMatrix[Double]]{

  override def zero = (DenseMatrix.zeros[Double](d, d), 0L)

  override def kernel(datum: (IndexT,  DenseVector[Double])):  (DenseMatrix[Double], Long) = {
    (datum._2 * datum._2.t, 1L)
  }

  override def reducer(r1: (DenseMatrix[Double], Long), r2: (DenseMatrix[Double], Long)): (DenseMatrix[Double], Long) = {
    (r1._1 + r2._1, r1._2 + r2._2)
  }

  def normalize(x: (DenseMatrix[Double], Long)): DenseMatrix[Double] = {
    x._1 / x._2.toDouble
  }

  override def windowEstimate(window: Array[(IndexT, DenseVector[Double])]): DenseMatrix[Double] = {
    normalize(windowStats(window))
  }

  override def blockEstimate(block: SingleAxisBlock[IndexT, DenseVector[Double]]): DenseMatrix[Double] = {
    normalize(blockStats(block))
  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): DenseMatrix[Double] = {
    normalize(timeSeriesStats(timeSeries))
  }

}

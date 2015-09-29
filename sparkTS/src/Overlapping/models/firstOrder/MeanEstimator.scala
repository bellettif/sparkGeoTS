package overlapping.models.firstOrder

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import overlapping.IntervalSize
import overlapping.containers.block.SingleAxisBlock
import overlapping.models.Estimator

/**
 * Created by Francois Belletti on 9/23/15.
 */
class MeanEstimator[IndexT <: Ordered[IndexT]](val d: Int)
  extends FirstOrderEssStat[IndexT, DenseVector[Double], (DenseVector[Double], Long)]
  with Estimator[IndexT, DenseVector[Double], DenseVector[Double]]{

  override def zero = (DenseVector.zeros[Double](d), 0L)

  override def kernel(datum: (IndexT,  DenseVector[Double])):  (DenseVector[Double], Long) = {
    (datum._2, 1L)
  }

  override def reducer(r1: (DenseVector[Double], Long), r2: (DenseVector[Double], Long)): (DenseVector[Double], Long) = {
    (r1._1 + r2._1, r1._2 + r2._2)
  }

  def normalize(x: (DenseVector[Double], Long)): DenseVector[Double] = {
    x._1 / x._2.toDouble
  }

  override def windowEstimate(window: Array[(IndexT, DenseVector[Double])]): DenseVector[Double] = {
    normalize(windowStats(window))
  }

  override def blockEstimate(block: SingleAxisBlock[IndexT, DenseVector[Double]]): DenseVector[Double] = {
    normalize(blockStats(block))
  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): DenseVector[Double] = {
    normalize(timeSeriesStats(timeSeries))
  }

}

package main.scala.overlapping.timeSeriesOld

import breeze.linalg.DenseVector
import main.scala.overlapping.containers._

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/23/15.
 */

object MeanEstimator{

  /**
   * Compute the mean of a Time Series RDD.
   *
   * @param timeSeries Input data.
   * @tparam IndexT Timestamp type.
   * @return Dimension-wise mean.
   */
  def apply[IndexT : ClassTag](timeSeries: VectTimeSeries[IndexT]): DenseVector[Double] ={

    val estimator = new MeanEstimator[IndexT](timeSeries.config)
    estimator.estimate(timeSeries)

  }

}

/**
 * This class is dedicated to estimating the mean of a distributed time series.
 *
 * @param config Configuration of the data.
 * @tparam IndexT Timestamp type.
 */
class MeanEstimator[IndexT : ClassTag](config: VectTSConfig[IndexT])
  extends FirstOrderEssStat[IndexT, (DenseVector[Double], Long)]
  with Estimator[IndexT, DenseVector[Double]]{

  override def zero = (DenseVector.zeros[Double](config.dim), 0L)

  override def kernel(t: TSInstant[IndexT], v: DenseVector[Double]):  (DenseVector[Double], Long) = {
    (v, 1L)
  }

  override def reducer(r1: (DenseVector[Double], Long), r2: (DenseVector[Double], Long)): (DenseVector[Double], Long) = {
    (r1._1 + r2._1, r1._2 + r2._2)
  }

  def normalize(x: (DenseVector[Double], Long)): DenseVector[Double] = {
    x._1 / x._2.toDouble
  }

  override def estimate(timeSeries: VectTimeSeries[IndexT]): DenseVector[Double] = {
    normalize(timeSeriesStats(timeSeries))
  }

}

package main.scala.overlapping.timeSeries

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import main.scala.overlapping._
import main.scala.overlapping.containers._

/**
 * Created by Francois Belletti on 9/23/15.
 */

object SecondMomentEstimator{

  /**
   * Compute the second moment of a Time Series RDD.
   *
   * @param timeSeries Input data.
   * @tparam IndexT Timestamp type.
   * @return Second moment matrix.
   */
  def apply[IndexT <: TSInstant[IndexT]](
      timeSeries: VectTimeSeries[IndexT]): DenseMatrix[Double] ={

    val estimator = new SecondMomentEstimator[IndexT](timeSeries.config)
    estimator.estimate(timeSeries)

  }

}

class SecondMomentEstimator[IndexT <: TSInstant[IndexT]](config: VectTSConfig[IndexT])
  extends FirstOrderEssStat[IndexT, DenseVector[Double], (DenseMatrix[Double], Long)]
  with Estimator[IndexT, DenseVector[Double], DenseMatrix[Double]]{

  override def zero = (DenseMatrix.zeros[Double](config.dim, config.dim), 0L)

  override def kernel(datum: (IndexT,  DenseVector[Double])):  (DenseMatrix[Double], Long) = {
    (datum._2 * datum._2.t, 1L)
  }

  override def reducer(r1: (DenseMatrix[Double], Long), r2: (DenseMatrix[Double], Long)): (DenseMatrix[Double], Long) = {
    (r1._1 + r2._1, r1._2 + r2._2)
  }

  def normalize(x: (DenseMatrix[Double], Long)): DenseMatrix[Double] = {
    x._1 / x._2.toDouble
  }

  override def estimate(timeSeries: TimeSeries[IndexT, DenseVector[Double]]): DenseMatrix[Double] = {
    normalize(timeSeriesStats(timeSeries))
  }

}

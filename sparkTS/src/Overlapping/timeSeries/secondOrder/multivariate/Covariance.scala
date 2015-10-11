package overlapping.timeSeries.secondOrder.multivariate

import breeze.linalg._
import org.apache.spark.rdd.RDD
import overlapping._
import overlapping.containers.SingleAxisBlock
import overlapping.timeSeries.{Estimator, ModelSize, SecondOrderEssStat}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/10/15.
 */

/*
Estimator of the instantaneous covariance of the process.
 */
class Covariance[IndexT <: Ordered[IndexT] : ClassTag](
     d: Int,
     mean: DenseVector[Double])
  extends SecondOrderEssStat[IndexT, DenseVector[Double], (DenseMatrix[Double], Long)]
  with Estimator[IndexT, DenseVector[Double], DenseMatrix[Double]]{

  override def kernelWidth = IntervalSize(0, 0)

  override def modelOrder = ModelSize(0, 0)

  override def zero = (DenseMatrix.zeros[Double](d, d), 0L)

  override def kernel(slice: Array[(IndexT, DenseVector[Double])]): (DenseMatrix[Double], Long) = {
    (slice(0)._2 * slice(0)._2.t, 1L)
  }

  override def reducer(x: (DenseMatrix[Double], Long), y: (DenseMatrix[Double], Long)): (DenseMatrix[Double], Long) ={
    (x._1 + y._1, x._2 + y._2)
  }

  def normalize(r: (DenseMatrix[Double], Long)): DenseMatrix[Double] = {
    r._1.map(_ / r._2.toDouble)
  }

  override def windowEstimate(slice: Array[(IndexT, DenseVector[Double])]):
    DenseMatrix[Double] = {

    normalize(
      windowStats(slice)
    )

  }

  override def blockEstimate(block: SingleAxisBlock[IndexT, DenseVector[Double]]):
    DenseMatrix[Double] ={

    normalize(
      blockStats(block)
    )

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]):
    DenseMatrix[Double] ={

    normalize(
      timeSeriesStats(timeSeries)
    )

  }


}
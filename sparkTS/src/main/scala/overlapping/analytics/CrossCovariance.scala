package main.scala.overlapping.analytics

import breeze.linalg._
import breeze.numerics._
import main.scala.overlapping.containers._
import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/10/15.
 */

/**
Here we expect the number of dimensions to be the same for all records.

The autocovoriance is ordered as follows

-modelOrder ... 0 ... modelOrder
  */

object CrossCovariance{

  def normalize(maxLag: Int, d: Int, N: Long, mean: DenseVector[Double])(
    covAndCount: (Array[DenseMatrix[Double]], Long)): Array[DenseMatrix[Double]] = {

    var i, c1, c2 = 0

    val cov = covAndCount._1
    val count = covAndCount._2

    val meanAdjustment = mean * mean.t

    val result = Array.fill(2 * maxLag + 1)(DenseMatrix.zeros[Double](d, d))

    // Lag zero
    result(maxLag) = (cov(maxLag) / count.toDouble) - meanAdjustment

    // Backward lags
    i = 0
    while(i < maxLag){
      result(i) = (cov(i) / count.toDouble) - meanAdjustment
      i += 1
    }

    // Forward lags
    i = 1
    while(i <= maxLag){
      result(maxLag + i) = (cov(maxLag - i).t / count.toDouble) - meanAdjustment.t
      i += 1
    }

    result

  }

  def selection[IndexT : TSInstant](bckPadding: IndexT)(
    target: IndexT, aux: IndexT): Boolean = {

    if(implicitly[TSInstant[IndexT]].compare(aux, target) == 1){
      false
    }else{
      val timeBtw = implicitly[TSInstant[IndexT]].timeBtw(aux, target)
      implicitly[TSInstant[IndexT]].compare(timeBtw, bckPadding) <= 0
    }

  }

  /**
   *  The kernel computes a contribution to the covariance estimator with the convention
   *  result(...) = cov(- (maxLag - 1), ..., 0)
   * @param maxLag
   * @param d
   * @param slice
   * @tparam IndexT
   * @return
   */
  def kernel[IndexT : TSInstant](maxLag: Int, d: Int)(
    slice: Array[(IndexT, DenseVector[Double])]): (Array[DenseMatrix[Double]], Long) = {

    val modelWidth = maxLag + 1

    val result = Array.fill(maxLag + 1)(DenseMatrix.zeros[Double](d, d))

    // The slice is not full size, it shall not be considered
    // so that the normalizing constant is similar for all autocov(h).
    if(slice.length != modelWidth){
      return (result, 0L)
    }

    val centerTarget  = slice(maxLag)._2

    var i, c1, c2 = 0
    while(i <= maxLag){

      val currentTarget = slice(i)._2
      c1 = 0
      while(c1 < d){
        c2 = 0
        while(c2 < d){
          result(i)(c1, c2) += centerTarget(c1) * currentTarget(c2)
          c2 += 1
        }
        c1 += 1
      }

      i += 1
    }

    (result, 1L)

  }

  def reduce(
      x: (Array[DenseMatrix[Double]], Long),
      y: (Array[DenseMatrix[Double]], Long)): (Array[DenseMatrix[Double]], Long) ={

    (x._1.zip(y._1).map({case (u, v) => u + v}), x._2 + y._2)

  }

  def apply[IndexT : TSInstant : ClassTag](
      timeSeries: SingleAxisVectTS[IndexT],
      maxLag: Int,
      mean: Option[DenseVector[Double]] = None): Array[DenseMatrix[Double]] = {

    val config = timeSeries.config
    val d = config.dim
    val deltaT = config.deltaT

    val bckPadding = implicitly[TSInstant[IndexT]].times(deltaT, maxLag)
    if (implicitly[TSInstant[IndexT]].compare(bckPadding, config.bckPadding) > 0) {
      throw new IndexOutOfBoundsException("Not enough padding to support cross covariance estimation. At least deltaT * maxLag is necessary.")
    }

    def zero: (Array[DenseMatrix[Double]], Long) = (Array.fill(2 * maxLag + 1){DenseMatrix.zeros[Double](d, d)}, 0L)

    normalize(maxLag, d, config.nSamples, mean.getOrElse(Average(timeSeries)))(
      timeSeries.slidingFold(selection(bckPadding))(
        kernel(maxLag, d),
        zero,
        reduce)
    )

  }

}


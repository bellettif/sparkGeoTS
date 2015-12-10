package main.scala.overlapping.analytics

import breeze.linalg._
import breeze.numerics.sqrt
import main.scala.overlapping.containers._

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/10/15.
 */

/**
Here we expect the number of dimensions to be the same for all records.

The autocovoriance is ordered as follows

-modelOrder ... 0 ... modelOrder
  */

object CrossCorrelation{

  def normalize(maxLag: Int, d: Int)(
      cov: Array[DenseMatrix[Double]]): Array[DenseMatrix[Double]] = {

    var i, c1, c2 = 0

    val covCpy: DenseMatrix[Double] = cov(maxLag).copy

    while(i < cov.length) {
      c1 = 0
      while (c1 < d) {
        c2 = c1
        while (c2 < d) {
          cov(i)(c1, c2) /= sqrt(covCpy(c1, c1) * covCpy(c2, c2))
          c2 += 1
        }
        c1 += 1
      }
      i += 1
    }

    i = 0
    c1 = 0
    c2 = 0

    while(i <= maxLag){
      c1 = 0
      while(c1 < d){
        c2 = 0
        while(c2 < c1) {
          cov(i)(c1, c2) = cov(i)(c2, c1)
          c2 += 1
        }
        c1 += 1
      }
      i += 1
    }

    i = 1
    while(i <= maxLag){
      cov(maxLag + i) = cov(maxLag - i).t
      i += 1
    }

    cov

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
      throw new IndexOutOfBoundsException("Not enough padding to support model estimation.")
    }

    def zero = (Array.fill(2 * maxLag + 1){DenseMatrix.zeros[Double](d, d)}, DenseVector.zeros[Double](d))

    normalize(maxLag, d)(CrossCovariance(timeSeries, maxLag, mean))

  }

}
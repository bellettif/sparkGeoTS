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

    val covCpy = diag(cov(maxLag)).copy

    while(i < cov.length) {
      c1 = 0
      while (c1 < d) {
        c2 = 0
        while (c2 < d) {
          cov(i)(c1, c2) /= sqrt(covCpy(c1) * covCpy(c2))
          c2 += 1
        }
        c1 += 1
      }
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

    val meanVal = Some(mean.getOrElse(Average(timeSeries)))

    normalize(maxLag, d)(CrossCovariance(timeSeries, maxLag, meanVal))

  }

}
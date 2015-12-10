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

  def selection[IndexT : TSInstant](bckPadding: IndexT)(
    target: IndexT, aux: IndexT): Boolean = {

    implicitly[TSInstant[IndexT]].compare(implicitly[TSInstant[IndexT]].timeBtw(aux, target), bckPadding) >= 0

  }

  def kernel[IndexT : TSInstant](maxLag: Int, d: Int, mean: Option[DenseVector[Double]] = None)(
    slice: Array[(IndexT, DenseVector[Double])]): Array[DenseMatrix[Double]] = {

    val modelWidth = 2 * maxLag + 1

    val result = Array.fill(modelWidth)(DenseMatrix.zeros[Double](d, d))

    // The slice is not full size, it shall not be considered in order to avoid redundant computations
    if(slice.length != modelWidth){
      return result
    }

    val meanValue = mean.getOrElse(DenseVector.zeros[Double](d))
    val centerTarget  = slice(maxLag)._2 - meanValue

    var i, c1, c2 = 0
    while(i <= maxLag){

      val currentTarget = slice(i)._2 - meanValue
      c1 = 0
      while(c1 < d){
        c2 = c1
        while(c2 < d){
          result(i)(c1, c2) += centerTarget(c1) * currentTarget(c2)
          c2 += 1
        }
        c1 += 1
      }

      i += 1
    }

    result

  }

  def reduce(
      x: Array[DenseMatrix[Double]],
      y: Array[DenseMatrix[Double]]): Array[DenseMatrix[Double]] ={

    x.zip(y).map({case (u, v) => u + v})

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

    def zero: Array[DenseMatrix[Double]] = Array.fill(2 * maxLag + 1){DenseMatrix.zeros[Double](d, d)}

    timeSeries.slidingFold(
      selection(bckPadding),
      kernel(maxLag, d, mean),
      zero,
      reduce)

  }

}


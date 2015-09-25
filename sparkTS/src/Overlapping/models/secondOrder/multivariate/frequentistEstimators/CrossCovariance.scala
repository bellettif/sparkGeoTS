package overlapping.models.secondOrder.multivariate.frequentistEstimators

import breeze.linalg._
import org.apache.spark.rdd.RDD
import overlapping.IntervalSize
import overlapping.containers.block.SingleAxisBlock
import overlapping.models.Estimator
import overlapping.models.secondOrder.{ModelSize, SecondOrderEssStat}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/10/15.
 */

/*
Here we expect the number of dimensions to be the same for all records.

The autocovoriance is ordered as follows

-modelOrder ... 0 ... modelOrder


 */
class CrossCovariance[IndexT <: Ordered[IndexT] : ClassTag](
     deltaT: Double,
     maxLag: Int,
     d: Int,
     mean: DenseVector[Double])
  extends Serializable
  with SecondOrderEssStat[IndexT, DenseVector[Double], (Array[DenseMatrix[Double]], Long)]
  with Estimator[IndexT, DenseVector[Double], Array[DenseMatrix[Double]]]{

  override def kernelWidth = IntervalSize(deltaT * maxLag, deltaT * maxLag)

  override def modelOrder = ModelSize(maxLag, maxLag)

  override def zero = (Array.fill(modelWidth){DenseMatrix.zeros[Double](d, d)}, 0L)

  override def kernel(slice: Array[(IndexT, DenseVector[Double])]): (Array[DenseMatrix[Double]], Long) = {

    val nCols  = slice(0)._2.length

    val result = Array.fill(modelOrder.lookBack + modelOrder.lookAhead + 1)(DenseMatrix.zeros[Double](nCols, nCols))

    /*
    The slice is not full size, it shall not be considered in order to avoid redundant computations
     */
    if(slice.length != modelWidth){
      return (result, 0L)
    }

    val centerTarget  = slice(modelOrder.lookBack)._2

    for(i <- 0 to modelOrder.lookBack){
      val currentTarget = slice(i)._2 - mean
      for(c1 <- 0 until nCols){
        for(c2 <- 0 until nCols){
          result(i)(c1, c2) += (centerTarget(c1) - mean(c1)) * currentTarget(c2)
        }
      }
    }

    for(i <- 1 to modelOrder.lookAhead){
      result(modelOrder.lookBack + i) = result(modelOrder.lookBack - i).t
    }

    (result, 1L)
  }

  override def reducer(x: (Array[DenseMatrix[Double]], Long), y: (Array[DenseMatrix[Double]], Long)): (Array[DenseMatrix[Double]], Long) ={
    (x._1.zip(y._1).map({case (u, v) => u :+ v}), x._2 + y._2)
  }

  def normalize(r: (Array[DenseMatrix[Double]], Long)): Array[DenseMatrix[Double]] = {
    r._1.map(_ / r._2.toDouble)
  }

  override def windowEstimate(slice: Array[(IndexT, DenseVector[Double])]):
    (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val covarianceMatrices = normalize(
      windowStats(slice)
    )

    (covarianceMatrices, covarianceMatrices(modelOrder.lookBack))

  }

  override def blockEstimate(block: SingleAxisBlock[IndexT, DenseVector[Double]]):
    (Array[DenseMatrix[Double]], DenseMatrix[Double])={

    val covarianceMatrices = normalize(
      blockStats(block)
    )

    (covarianceMatrices, covarianceMatrices(modelOrder.lookBack))

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]):
    (Array[DenseMatrix[Double]], DenseMatrix[Double])={

    val covarianceMatrices = normalize(
      timeSeriesStats(timeSeries)
    )

    (covarianceMatrices, covarianceMatrices(modelOrder.lookBack))

  }


}
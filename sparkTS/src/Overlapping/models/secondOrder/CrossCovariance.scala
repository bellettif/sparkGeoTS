package overlapping.models.secondOrder

import breeze.linalg._
import org.apache.spark.rdd.RDD
import overlapping.IntervalSize
import overlapping.containers.block.SingleAxisBlock

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/10/15.
 */

/*
Here we expect the number of dimensions to be the same for all records.
 */
class CrossCovariance[IndexT <: Ordered[IndexT] : ClassTag](deltaT: Double, modelOrder: Int)
  extends Serializable with SecondOrderModel[IndexT, Array[Double]]{

  def computeCrossCov(slice: Array[(IndexT, Array[Double])]): (Array[DenseMatrix[Double]], Long) = {

    val nCols  = slice(0)._2.length

    val result = Array.fill(2 * modelOrder + 1)(DenseMatrix.zeros[Double](nCols, nCols))

    /*
    The slice is not full size, it shall not be considered in order to avoid redundant computations
     */
    if(slice.length != 2 * modelOrder + 1){
      return (result, 0L)
    }

    val centerTarget  = slice(modelOrder)._2

    for(i <- 0 until 2 * modelOrder + 1){
      val currentTarget = slice(i)._2
      for(c1 <- 0 until nCols){
        for(c2 <- 0 until nCols){
          result(i)(c1, c2) += centerTarget(c1) * currentTarget(c2)
        }
      }
    }

    (result, 1L)
  }

  def sumArrays(x: (Array[DenseMatrix[Double]], Long), y: (Array[DenseMatrix[Double]], Long)): (Array[DenseMatrix[Double]], Long) ={
    (x._1.zip(y._1).map({case (u, v) => u :+ v}), x._2 + y._2)
  }

  def computeCovariations(timeSeries: SingleAxisBlock[IndexT, Array[Double]]): (Array[DenseMatrix[Double]], Long) ={

    val nCols = timeSeries.take(1).head._2.length

    val zero = (Array.fill(2 * modelOrder + 1)(DenseMatrix.zeros[Double](nCols, nCols)), 0L)

    val selectionSize = IntervalSize(modelOrder * deltaT, modelOrder * deltaT)

    timeSeries
      .slidingFold(Array(selectionSize))(computeCrossCov, zero, sumArrays)

  }

  def normalize(r: (Array[DenseMatrix[Double]], Long)): Array[DenseMatrix[Double]] = {
    r._1.map(_ / r._2.toDouble)
  }

  override def estimate(slice: Array[(IndexT, Array[Double])]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val covarianceMatrices = normalize(
      slice.sliding(2 * modelOrder + 1)
        .map(computeCrossCov)
        .reduce(sumArrays)
    )

    (covarianceMatrices, covarianceMatrices(modelOrder))

  }

  override def estimate(timeSeries: SingleAxisBlock[IndexT, Array[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double])={

    val covarianceMatrices = normalize(
      computeCovariations(timeSeries)
    )

    (covarianceMatrices, covarianceMatrices(modelOrder))

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, Array[Double]])]): (Array[DenseMatrix[Double]], DenseMatrix[Double])={

    val covarianceMatrices = normalize(
      timeSeries
        .mapValues(computeCovariations)
        .map(_._2)
        .reduce(sumArrays)
    )

    (covarianceMatrices, covarianceMatrices(modelOrder))

  }


}
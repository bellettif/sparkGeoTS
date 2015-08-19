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
class CrossCovariance[IndexT <: Ordered[IndexT] : ClassTag](selectionSize: IntervalSize, modelOrder: Int)
  extends Serializable with SecondOrderModel[IndexT, Array[Double]]{

  def computeCrossCov(slice: Array[(IndexT, Array[Double])]): (Array[DenseMatrix[Double]], Long) = {

    val nCols         = slice(0)._2.length
    val centerTarget  = slice(modelOrder)._2

    val result = Array.fill(2 * modelOrder + 1)(DenseMatrix.zeros[Double](nCols, nCols))

    if(slice.length != 2 * modelOrder + 1){
      return (result, 0L)
    }

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

    timeSeries
      .slidingFold(Array(selectionSize))(computeCrossCov, zero, sumArrays)

  }

  override def estimate(timeSeries: SingleAxisBlock[IndexT, Array[Double]]): Array[DenseMatrix[Double]]={

    val (covariations, nSamples) = computeCovariations(timeSeries)
    covariations.map(_ / nSamples.toDouble)

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, Array[Double]])]): Array[DenseMatrix[Double]]={

    val (covariations, nSamples) = timeSeries
      .mapValues(computeCovariations)
      .map(_._2)
      .reduce(sumArrays)

    covariations.map(_ / nSamples.toDouble)

  }


}
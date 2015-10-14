package overlapping.timeSeries

import breeze.linalg._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import overlapping._
import overlapping.containers.SingleAxisBlock

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/10/15.
 */

/**
Here we expect the number of dimensions to be the same for all records.

The autocovoriance is ordered as follows

-modelOrder ... 0 ... modelOrder
 */

class CrossCovariance[IndexT <: Ordered[IndexT] : ClassTag](
    maxLag: Int,
    mean: Option[DenseVector[Double]] = None)
    (implicit config: TSConfig, sc: SparkContext)
  extends SecondOrderEssStat[IndexT, DenseVector[Double], (Array[DenseMatrix[Double]], Long)]
  with Estimator[IndexT, DenseVector[Double], (Array[DenseMatrix[Double]], DenseMatrix[Double])]{

  val d = config.d
  val deltaT = config.deltaT
  val bcMean = sc.broadcast(if (mean.isDefined) mean.get else DenseVector.zeros[Double](d))

  override def kernelWidth = IntervalSize(deltaT * maxLag, deltaT * maxLag)

  override def modelOrder = ModelSize(maxLag, maxLag)

  override def zero = (Array.fill(modelWidth){DenseMatrix.zeros[Double](d, d)}, 0L)

  override def kernel(slice: Array[(IndexT, DenseVector[Double])]): (Array[DenseMatrix[Double]], Long) = {

    val result = Array.fill(modelOrder.lookBack + modelOrder.lookAhead + 1)(DenseMatrix.zeros[Double](d, d))

    /*
    The slice is not full size, it shall not be considered in order to avoid redundant computations
     */
    if(slice.length != modelWidth){
      return (result, 0L)
    }

    val meanValue = bcMean.value
    val centerTarget  = slice(modelOrder.lookBack)._2 - meanValue

    for(i <- 0 to modelOrder.lookBack){
      val currentTarget = slice(i)._2 - meanValue
      for(c1 <- 0 until d){
        for(c2 <- 0 until d){
          result(i)(c1, c2) += centerTarget(c1) * currentTarget(c2)
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

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]):
    (Array[DenseMatrix[Double]], DenseMatrix[Double])={

    val covarianceMatrices = normalize(
      timeSeriesStats(timeSeries)
    )

    (covarianceMatrices, covarianceMatrices(modelOrder.lookBack))

  }


}
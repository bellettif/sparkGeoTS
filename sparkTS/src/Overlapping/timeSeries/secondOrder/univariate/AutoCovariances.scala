package overlapping.timeSeries.secondOrder.univariate

import breeze.linalg.{DenseVector, reverse}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import overlapping._
import overlapping.containers.SingleAxisBlock
import overlapping.timeSeries.{Estimator, ModelSize, SecondOrderEssStat}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/25/15.
 */
class AutoCovariances[IndexT <: Ordered[IndexT] : ClassTag](
    deltaT: Double,
    maxLag: Int,
    d: Int,
    mean: Broadcast[DenseVector[Double]]
  )
  extends SecondOrderEssStat[IndexT, DenseVector[Double], (Array[CovSignature], Long)]
  with Estimator[IndexT, DenseVector[Double], Array[CovSignature]]
{

  def kernelWidth = IntervalSize(deltaT * maxLag, 0)

  def modelOrder = ModelSize(maxLag, 0)

  def zero = (Array.fill(d){CovSignature(DenseVector.zeros[Double](modelWidth), 0.0)}, 0L)

  override def kernel(slice: Array[(IndexT, DenseVector[Double])]): (Array[CovSignature], Long) = {

    val tempCovs = Array.fill(d){DenseVector.zeros[Double](modelWidth)}
    val tempVars = Array.fill(d){0.0}

    val meanValue = mean.value

    /*
    The slice is not full size, it shall not be considered in order to avoid redundant computations
     */
    if(slice.length != modelWidth){
      return (Array.fill(d){CovSignature(DenseVector.zeros[Double](modelWidth), 0.0)}, 0L)
    }

    for(c <- 0 until d){

      val centerTarget  = slice(modelOrder.lookBack)._2(c) - meanValue(c)
      tempVars(c) += centerTarget * centerTarget

      for(i <- 0 to modelOrder.lookBack){
        tempCovs(c)(i) += centerTarget * (slice(i)._2(c) - meanValue(c))
      }

    }

    (tempCovs.zip(tempVars).map({case (x, y) => CovSignature(x, y)}), 1L)

  }

  override def reducer(x: (Array[CovSignature], Long), y: (Array[CovSignature], Long)):
    (Array[CovSignature], Long) = {

    (x._1.zip(y._1).map({case (CovSignature(cov1, v1), CovSignature(cov2, v2)) => CovSignature(cov1 + cov2, v1 + v2)}), x._2 + y._2)

  }

  override def windowEstimate(window: Array[(IndexT, DenseVector[Double])]):
    Array[CovSignature] = {

    val (covSigns: Array[CovSignature], nSamples: Long) = windowStats(window)

    covSigns.map(x => CovSignature(reverse(x.covariation) / nSamples.toDouble, x.variation / nSamples.toDouble))

  }

  override def blockEstimate(block: SingleAxisBlock[IndexT, DenseVector[Double]]):
    Array[CovSignature] = {

    val (covSigns: Array[CovSignature], nSamples: Long) = blockStats(block)

    covSigns.map(x => CovSignature(reverse(x.covariation) / nSamples.toDouble, x.variation / nSamples.toDouble))

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]):
    Array[CovSignature] = {

    val (covSigns: Array[CovSignature], nSamples: Long) = timeSeriesStats(timeSeries)

    covSigns.map(x => CovSignature(reverse(x.covariation) / nSamples.toDouble, x.variation / nSamples.toDouble))

  }

}

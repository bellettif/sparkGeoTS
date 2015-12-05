package main.scala.overlapping.timeSeries

import breeze.linalg._
import breeze.numerics.abs
import main.scala.overlapping.containers._
import org.apache.spark.broadcast.Broadcast
import breeze.math._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 12/2/15.
 */

object LogPeriodogram{

  /**
   *  Estimator of the instantaneous covariance of the process E[X_t transpose(X_t)].
   */
  def apply[IndexT <: TSInstant[IndexT] : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      maxLag: Int,
      mean: Option[DenseVector[Double]] = None)
      (implicit config: TSConfig): DenseMatrix[Complex] ={

    val estimator = new LogPeriodogram[IndexT](
      maxLag,
      timeSeries.config,
      timeSeries.content.context.broadcast(mean))

    estimator.estimate(timeSeries)

  }

}

class LogPeriodogram[IndexT <: TSInstant[IndexT] : ClassTag](
     maxLag: Int,
     config: VectTSConfig[IndexT],
     bcMean: Broadcast[Option[DenseVector[Double]]])
  extends SecondOrderEssStat[IndexT, DenseVector[Double], DenseMatrix[Complex]]
  with Estimator[IndexT, DenseVector[Double], DenseMatrix[Complex]]{

  override def selection = config.selection

  override def modelOrder = ModelSize(maxLag, maxLag)

  override def zero = DenseMatrix.zeros[Complex](config.dim, 2 * maxLag + 1)

  override def kernel(slice: Array[(IndexT, DenseVector[Double])]): DenseMatrix[Complex] = {

    val result = DenseMatrix.zeros[Complex](config.dim, 2 * maxLag + 1)

    if(slice.length != modelWidth){
      return result
    }

    val buffer = DenseVector.zeros[Double](2 * maxLag + 1)

    val mask = - linspace(-1.0, 1.0, 2 * maxLag + 1) :* linspace(-1.0, 1.0, 2 * maxLag + 1) :+ 1.0

    var idx = 0
    var t = 0
    while(idx < config.dim){
      t = 0
      while(t < 2 * maxLag + 1){
        buffer(t) = slice(t)._2(idx)
        t += 1
      }
      buffer :*= mask
      val temp: DenseVector[Complex] = breeze.signal.fourierTr[DenseVector[Double], DenseVector[Complex]](buffer)
      t = 0
      while(t < 2 * maxLag + 1){
        result(idx, t) = temp(t)
        t += 1
      }

      val check = breeze.signal.iFourierTr[DenseVector[Complex], DenseVector[Complex]](temp)
      idx += 1
    }

    result

  }

  override def reducer(x: DenseMatrix[Complex], y: DenseMatrix[Complex]): DenseMatrix[Complex] ={
    x + y
  }

  override def estimate(timeSeries: TimeSeries[IndexT, DenseVector[Double]]):
  DenseMatrix[Complex] ={

    val result = timeSeriesStats(timeSeries)

    for(i <- 0 until config.dim){
      result(i, ::) :/ convert(sum(abs(result(i, ::).t) :* abs(result(i, ::).t)), Complex)
    }

    result

  }

}

package main.scala.overlapping.timeSeriesOld

import breeze.linalg.{DenseVector, reverse}
import main.scala.overlapping.containers._
import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/25/15.
 */
object AutoCovariances {

  def apply[IndexT : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      maxLag: Int,
      mean: Option[DenseVector[Double]] = None): Array[SecondOrderSignature] = {

    val estimator = new AutoCovariances[IndexT](
      maxLag,
      timeSeries.config,
      timeSeries.content.context.broadcast(mean))

    estimator.estimate(timeSeries)

  }

}


class AutoCovariances[IndexT : ClassTag](
    maxLag: Int,
    config: VectTSConfig[IndexT],
    bcMean: Broadcast[Option[DenseVector[Double]]])
  extends SecondOrderEssStat[IndexT, (Array[SecondOrderSignature], Long)]
  with Estimator[IndexT, Array[SecondOrderSignature]]
{

  val d = config.dim

  val deltaT = config.deltaT

  if(deltaT * maxLag > config.bckPadding){
    throw new IndexOutOfBoundsException("Not enough padding to support model estimation.")
  }

  override def selection = config.selection

  override def modelOrder = ModelSize(maxLag, 0)

  override def zero = (Array.fill(d){SecondOrderSignature(DenseVector.zeros[Double](modelWidth), 0.0)}, 0L)

  override def kernel(slice: Array[(TSInstant[IndexT], DenseVector[Double])]): (Array[SecondOrderSignature], Long) = {

    val tempCovs = Array.fill(d){DenseVector.zeros[Double](modelWidth)}
    val tempVars = Array.fill(d){0.0}

    val meanValue = bcMean.value.getOrElse(DenseVector.zeros[Double](d))

    /*
    The slice is not full size, this can happen at the very beginning of the data set.
     */
    if(slice.length != modelWidth){
      return (Array.fill(d){SecondOrderSignature(DenseVector.zeros[Double](modelWidth), 0.0)}, 0L)
    }

    for(c <- 0 until d){

      val centerTarget  = slice(modelOrder.lookBack)._2(c) - meanValue(c)
      tempVars(c) += centerTarget * centerTarget

      for(i <- 0 to modelOrder.lookBack){
        tempCovs(c)(i) += centerTarget * (slice(i)._2(c) - meanValue(c))
      }

    }

    (tempCovs.zip(tempVars).map({case (x, y) => SecondOrderSignature(x, y)}), 1L)

  }

  override def reducer(x: (Array[SecondOrderSignature], Long), y: (Array[SecondOrderSignature], Long)):
    (Array[SecondOrderSignature], Long) = {

    (x._1.zip(y._1).map({case (SecondOrderSignature(cov1, v1), SecondOrderSignature(cov2, v2)) =>
      SecondOrderSignature(cov1 + cov2, v1 + v2)}), x._2 + y._2)

  }


  override def estimate(timeSeries: VectTimeSeries[IndexT]):
    Array[SecondOrderSignature] = {

    val (covSigns: Array[SecondOrderSignature], nSamples: Long) = timeSeriesStats(timeSeries)

    covSigns.map(x => SecondOrderSignature(reverse(x.covariation) / nSamples.toDouble, x.variation / nSamples.toDouble))

  }

}

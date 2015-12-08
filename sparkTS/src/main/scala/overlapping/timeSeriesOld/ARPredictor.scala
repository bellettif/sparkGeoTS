package main.scala.overlapping.timeSeriesOld

import breeze.linalg.DenseVector
import main.scala.overlapping.containers._
import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
object ARPredictor{

  def apply[IndexT : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      matrices: Array[DenseVector[Double]],
      mean: Option[DenseVector[Double]]): VectTimeSeries[IndexT] = {

    val predictor = new ARPredictor[IndexT](
      timeSeries.config,
      timeSeries.content.context.broadcast(matrices),
      timeSeries.content.context.broadcast(mean))

    predictor.estimateResiduals(timeSeries)

  }

}

class ARPredictor[IndexT : ClassTag](
    config: VectTSConfig[IndexT],
    bcMatrices: Broadcast[Array[DenseVector[Double]]],
    bcMean: Broadcast[Option[DenseVector[Double]]])
  extends Predictor[IndexT]{

  val p = bcMatrices.value(0).length

  val d = bcMatrices.value.length
  if(d != config.dim){
    throw new IndexOutOfBoundsException("AR matrix dimensions and time series dimension not compatible.")
  }

  def selection = config.selection

  override def predictKernel(data: Array[(TSInstant[IndexT], DenseVector[Double])]): DenseVector[Double] = {

    val pred = bcMean.value.getOrElse(DenseVector.zeros[Double](d)).copy
    val mean = bcMean.value.getOrElse(DenseVector.zeros[Double](d))

    for (i <- 0 until (data.length - 1)) {
      for (j <- 0 until d) {
        pred(j) += bcMatrices.value(j)(data.length - 2 - i) * (data(i)._2(j) - mean(j))
      }
    }

    pred

  }

}
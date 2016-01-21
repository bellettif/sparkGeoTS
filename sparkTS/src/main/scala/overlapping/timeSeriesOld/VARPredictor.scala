package main.scala.overlapping.timeSeriesOld

import breeze.linalg.{DenseMatrix, DenseVector}
import main.scala.overlapping.containers._
import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
object VARPredictor{

  def apply[IndexT : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      matrices: Array[DenseMatrix[Double]],
      mean: Option[DenseVector[Double]] = None): VectTimeSeries[IndexT] = {

    val predictor = new VARPredictor[IndexT](
      timeSeries.content.context.broadcast(matrices),
      timeSeries.content.context.broadcast(mean),
      timeSeries.config)

    predictor.estimateResiduals(timeSeries)

  }

}

class VARPredictor[IndexT : ClassTag](
    bcMatrices: Broadcast[Array[DenseMatrix[Double]]],
    bcMean: Broadcast[Option[DenseVector[Double]]],
    config: VectTSConfig[IndexT])
  extends Predictor[IndexT]{

  val p = bcMatrices.value.length
  val deltaT = config.deltaT
  val d = bcMatrices.value(0).rows
  if(d != config.dim){
    throw new IndexOutOfBoundsException("AR matrix dimensions and time series dimension not compatible.")
  }

  override def selection = config.selection

  override def predictKernel(data: Array[(TSInstant[IndexT], DenseVector[Double])]): DenseVector[Double] = {

    val pred = bcMean.value.getOrElse(DenseVector.zeros[Double](d)).copy

    for(i <- 0 until (data.length - 1)){
      pred :+= bcMatrices.value(data.length - 2 - i) * (data(i)._2 - bcMean.value.getOrElse(DenseVector.zeros[Double](d)))
    }

    pred

  }

}
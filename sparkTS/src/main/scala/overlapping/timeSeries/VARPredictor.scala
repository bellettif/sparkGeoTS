package main.scala.overlapping.timeSeries

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import main.scala.overlapping._
import main.scala.overlapping.containers._

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
object VARPredictor{

  def apply[IndexT <: TSInstant[IndexT] : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      matrices: Array[DenseMatrix[Double]],
      mean: Option[DenseVector[Double]])
     (implicit config: TSConfig): RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])] = {

    val predictor = new VARPredictor[IndexT](
      timeSeries.content.context.broadcast(matrices),
      timeSeries.content.context.broadcast(mean),
      timeSeries.config)
    predictor.estimateResiduals(timeSeries)

  }

}

class VARPredictor[IndexT <: TSInstant[IndexT] : ClassTag](
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

  override def predictKernel(data: Array[(IndexT, DenseVector[Double])]): DenseVector[Double] = {

    val pred = bcMean.value.getOrElse(DenseVector.zeros[Double](d)).copy

    for(i <- 0 until (data.length - 1)){
      pred :+= bcMatrices.value(data.length - 2 - i) * (data(i)._2 - bcMean.value.getOrElse(DenseVector.zeros[Double](d)))
    }

    pred

  }

}
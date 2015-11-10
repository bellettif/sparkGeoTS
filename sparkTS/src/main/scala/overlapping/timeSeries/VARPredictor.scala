package main.scala.overlapping.timeSeries

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import main.scala.overlapping._
import main.scala.overlapping.containers.SingleAxisBlock

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
object VARPredictor{

  def apply[IndexT <: Ordered[IndexT] : ClassTag](
      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
      matrices: Array[DenseMatrix[Double]],
      mean: Option[DenseVector[Double]])
     (implicit config: TSConfig): RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])] = {

    val predictor = new VARPredictor[IndexT](timeSeries.context.broadcast(matrices), timeSeries.context.broadcast(mean))
    predictor.estimateResiduals(timeSeries)

  }

}

class VARPredictor[IndexT <: Ordered[IndexT] : ClassTag](
    bcMatrices: Broadcast[Array[DenseMatrix[Double]]],
    bcMean: Broadcast[Option[DenseVector[Double]]])
    (implicit config: TSConfig)
  extends Predictor[IndexT]{

  val p = bcMatrices.value.length
  val deltaT = config.deltaT
  val d = bcMatrices.value(0).rows
  if(d != config.d){
    throw new IndexOutOfBoundsException("AR matrix dimensions and time series dimension not compatible.")
  }

  def size: Array[IntervalSize] = Array(IntervalSize(p * deltaT, 0))

  override def predictKernel(data: Array[(IndexT, DenseVector[Double])]): DenseVector[Double] = {

    val pred = bcMean.value.getOrElse(DenseVector.zeros[Double](d)).copy

    for(i <- 0 until (data.length - 1)){
      pred :+= bcMatrices.value(data.length - 2 - i) * (data(i)._2 - bcMean.value.getOrElse(DenseVector.zeros[Double](d)))
    }

    pred

  }

}
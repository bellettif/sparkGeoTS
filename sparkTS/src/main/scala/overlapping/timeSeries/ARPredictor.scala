package main.scala.overlapping.timeSeries

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import main.scala.overlapping._
import main.scala.overlapping.containers.SingleAxisBlock

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
object ARPredictor{

  def apply[IndexT <: Ordered[IndexT] : ClassTag](
      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
      matrices: Array[DenseVector[Double]],
      mean: Option[DenseVector[Double]])
      (implicit config: TSConfig): RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])] = {

    val predictor = new ARPredictor[IndexT](timeSeries.context.broadcast(matrices), timeSeries.context.broadcast(mean))
    predictor.estimateResiduals(timeSeries)

  }

}

class ARPredictor[IndexT <: Ordered[IndexT] : ClassTag](
    bcMatrices: Broadcast[Array[DenseVector[Double]]],
    bcMean: Broadcast[Option[DenseVector[Double]]])
    (implicit config: TSConfig)
  extends Predictor[IndexT]{

  val p = bcMatrices.value(0).length
  val deltaT = config.deltaT
  val d = bcMatrices.value.length
  if(d != config.d){
    throw new IndexOutOfBoundsException("AR matrix dimensions and time series dimension not compatible.")
  }

  def size: Array[IntervalSize] = Array(IntervalSize(p * deltaT, 0))

  override def predictKernel(data: Array[(IndexT, DenseVector[Double])]): DenseVector[Double] = {

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
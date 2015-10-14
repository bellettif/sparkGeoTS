package overlapping.timeSeries

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import overlapping._

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
class VARPredictor[IndexT <: Ordered[IndexT] : ClassTag](
    matrices: Array[DenseMatrix[Double]],
    mean: Option[DenseVector[Double]] = None)
    (implicit config: TSConfig, sc: SparkContext)
  extends Predictor[IndexT]{

  val p = matrices.length
  val deltaT = config.deltaT
  val d = matrices(0).rows
  if(d != config.d){
    throw new IndexOutOfBoundsException("AR matrix dimensions and time series dimension not compatible.")
  }
  val bcMatrices = sc.broadcast(matrices)
  val bcMean = sc.broadcast(mean.getOrElse(DenseVector.zeros[Double](d)))

  def size: Array[IntervalSize] = Array(IntervalSize(p * deltaT, 0))

  override def predictKernel(data: Array[(IndexT, DenseVector[Double])]): DenseVector[Double] = {

    val pred = bcMean.value.copy

    for(i <- 0 until (data.length - 1)){
      pred :+= bcMatrices.value(data.length - 2 - i) * (data(i)._2 - bcMean.value)
    }

    pred

  }

}
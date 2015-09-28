package overlapping.models.secondOrder.multivariate

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.broadcast.Broadcast
import overlapping.IntervalSize
import overlapping.models.Predictor

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
class VARPredictor[IndexT <: Ordered[IndexT] : ClassTag](
    deltaT: Double,
    p: Int,
    d: Int,
    mean: Broadcast[DenseVector[Double]],
    matrices: Broadcast[Array[DenseMatrix[Double]]]
  )
  extends Predictor[IndexT]{

  def size: Array[IntervalSize] = Array(IntervalSize(p * deltaT, 0))

  override def predictKernel(data: Array[(IndexT, DenseVector[Double])]): DenseVector[Double] = {

    val pred = mean.value.copy
    for(i <- 0 until (data.length - 1)){
      pred :+= matrices.value(data.length - 2 - i) * (data(i)._2 - mean.value)
    }

    pred

  }

}
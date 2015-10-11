package overlapping.timeSeries.secondOrder.univariate

import breeze.linalg.DenseVector
import org.apache.spark.broadcast.Broadcast
import overlapping._
import overlapping.timeSeries.Predictor

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
class ARPredictor[IndexT <: Ordered[IndexT] : ClassTag](
    deltaT: Double,
    p: Int,
    d: Int,
    mean: Broadcast[DenseVector[Double]],
    matrices: Broadcast[Array[DenseVector[Double]]]
  )
  extends Predictor[IndexT]{

  def size: Array[IntervalSize] = Array(IntervalSize(p * deltaT, 0))

  override def predictKernel(data: Array[(IndexT, DenseVector[Double])]): DenseVector[Double] = {

    val pred = mean.value.copy

    for (i <- 0 until (data.length - 1)) {
      for (j <- 0 until d) {
        pred(j) += matrices.value(j)(data.length - 2 - i) * (data(i)._2(j) - mean.value(j))
      }
    }

    pred

  }

}
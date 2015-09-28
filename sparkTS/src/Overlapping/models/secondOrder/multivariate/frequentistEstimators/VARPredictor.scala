package overlapping.models.secondOrder.multivariate.frequentistEstimators

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import overlapping.IntervalSize
import overlapping.containers.block.SingleAxisBlock
import overlapping.models.Predictor
import overlapping.models.secondOrder.multivariate.frequentistEstimators.procedures.RybickiMulti

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
class VARPredictor[IndexT <: Ordered[IndexT] : ClassTag](
    deltaT: Double,
    p: Int,
    d: Int,
    mean: DenseVector[Double],
    matrices: Array[DenseMatrix[Double]]
  )
  extends Predictor[IndexT]{

  def size: Array[IntervalSize] = Array(IntervalSize(p * deltaT, 0))

  override def predictKernel(data: Array[(IndexT, DenseVector[Double])]): DenseVector[Double] = {

    val pred = mean.copy
    for(i <- 0 until (data.length - 1)){
      pred :+= matrices(data.length - 2 - i) * (data(i)._2 - mean)
    }

    pred

  }

}
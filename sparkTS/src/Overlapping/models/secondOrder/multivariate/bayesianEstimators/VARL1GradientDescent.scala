package overlapping.models.secondOrder.multivariate.bayesianEstimators

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import overlapping.IntervalSize
import overlapping.containers.block.SingleAxisBlock
import overlapping.models.Estimator
import overlapping.models.secondOrder.SecondOrderEssStat
import overlapping.models.secondOrder.multivariate.bayesianEstimators.procedures.{GradientDescent, L1ClippedGradientDescent}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/16/15.
 */
class VARL1GradientDescent[IndexT <: Ordered[IndexT] : ClassTag](
    val p: Int,
    val deltaT: Double,
    val loss: AutoregressiveLoss[IndexT],
    val gradient: AutoregressiveGradient[IndexT],
    val stepSize: Int => Double,
    val precision: Double,
    val lambda: Double,
    val maxIter: Int,
    val start: Array[DenseMatrix[Double]])
  extends Estimator[IndexT, DenseVector[Double], Array[DenseMatrix[Double]]]{

  override def windowEstimate(slice: Array[(IndexT, DenseVector[Double])]): Array[DenseMatrix[Double]] = {
    L1ClippedGradientDescent.run[Array[(IndexT, DenseVector[Double])]](
      {case (param, data) => loss.setNewX(param); loss.windowStats(data)},
      {case (param, data) => gradient.setNewX(param); gradient.windowStats(data)},
      gradient.getGradientSize,
      stepSize,
      precision,
      lambda,
      maxIter,
      start,
      slice
    )
  }

  override def blockEstimate(block: SingleAxisBlock[IndexT, DenseVector[Double]]): Array[DenseMatrix[Double]] = {
    L1ClippedGradientDescent.run[SingleAxisBlock[IndexT, DenseVector[Double]]](
      {case (param, data) => loss.setNewX(param); loss.blockStats(data)},
      {case (param, data) => gradient.setNewX(param); gradient.blockStats(data)},
      gradient.getGradientSize,
      stepSize,
      precision,
      lambda,
      maxIter,
      start,
      block
    )
  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): Array[DenseMatrix[Double]] = {

    timeSeries.persist(MEMORY_AND_DISK)

    val parameters = L1ClippedGradientDescent.run[RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]](
      {case (param, data) => loss.setNewX(param); loss.timeSeriesStats(data)},
      {case (param, data) => gradient.setNewX(param); gradient.timeSeriesStats(data)},
      gradient.getGradientSize,
      stepSize,
      precision,
      lambda,
      maxIter,
      start,
      timeSeries
    )

    timeSeries.unpersist(false)

    parameters

  }

}

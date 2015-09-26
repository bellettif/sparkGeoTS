package overlapping.models.secondOrder.multivariate.frequentistEstimators

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import overlapping.containers.block.SingleAxisBlock
import overlapping.models.secondOrder.multivariate.frequentistEstimators.procedures.InnovationAlgoMulti

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
class VMAModel[IndexT <: Ordered[IndexT] : ClassTag](
    deltaT: Double,
    q: Int,
    d: Int,
    mean: DenseVector[Double]
  )
  extends CrossCovariance[IndexT](deltaT, q, d, mean){

  def estimateVMAMatrices(crossCovMatrices: Array[DenseMatrix[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) ={

    InnovationAlgoMulti(q, crossCovMatrices)

  }

  override def windowEstimate(slice: Array[(IndexT, DenseVector[Double])]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val crossCovMatrices: Array[DenseMatrix[Double]] = super.windowEstimate(slice)._1
    estimateVMAMatrices(crossCovMatrices)

  }

  override def blockEstimate(block: SingleAxisBlock[IndexT, DenseVector[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val crossCovMatrices: Array[DenseMatrix[Double]] = super.blockEstimate(block)._1
    estimateVMAMatrices(crossCovMatrices)

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): (Array[DenseMatrix[Double]], DenseMatrix[Double])= {

    val crossCovMatrices: Array[DenseMatrix[Double]] = super.estimate(timeSeries)._1
    estimateVMAMatrices(crossCovMatrices)

  }


}
package overlapping.models.secondOrder.multivariate.frequentistEstimators

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import overlapping.containers.block.SingleAxisBlock
import overlapping.models.secondOrder.multivariate.frequentistEstimators.procedures.InnovationAlgoMulti

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
class VMAModel[IndexT <: Ordered[IndexT] : ClassTag](deltaT: Double, modelOrder: Int)
  extends CrossCovariance[IndexT](deltaT, modelOrder){

  def estimateVMAMatrices(crossCovMatrices: Array[DenseMatrix[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) ={

    InnovationAlgoMulti(modelOrder, crossCovMatrices)

  }

  override def estimate(slice: Array[(IndexT, DenseVector[Double])]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val crossCovMatrices: Array[DenseMatrix[Double]] = super.estimate(slice)._1
    estimateVMAMatrices(crossCovMatrices)

  }

  override def estimate(timeSeries: SingleAxisBlock[IndexT, DenseVector[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val crossCovMatrices: Array[DenseMatrix[Double]] = super.estimate(timeSeries)._1
    estimateVMAMatrices(crossCovMatrices)

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): (Array[DenseMatrix[Double]], DenseMatrix[Double])= {

    val crossCovMatrices: Array[DenseMatrix[Double]] = super.estimate(timeSeries)._1
    estimateVMAMatrices(crossCovMatrices)

  }


}
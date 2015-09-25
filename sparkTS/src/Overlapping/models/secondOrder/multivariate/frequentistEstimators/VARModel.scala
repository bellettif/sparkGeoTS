package overlapping.models.secondOrder.multivariate.frequentistEstimators

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import overlapping.containers.block.SingleAxisBlock
import overlapping.models.secondOrder.multivariate.frequentistEstimators.procedures.RybickiMulti

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
class VARModel[IndexT <: Ordered[IndexT] : ClassTag](deltaT: Double, modelOrder: Int)
  extends CrossCovariance[IndexT](deltaT, modelOrder){

  def estimateVARMatrices(crossCovMatrices: Array[DenseMatrix[Double]], covMatrix: DenseMatrix[Double]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) ={
    val nCols = covMatrix.rows

    val coeffMatrices = RybickiMulti(modelOrder, nCols,
      crossCovMatrices.slice(1, 2 * modelOrder),
      crossCovMatrices.slice(modelOrder + 1, 2 * modelOrder + 1))

    coeffMatrices.foreach(x => x := x.t)

    var noiseVariance = covMatrix
    for(i <- 1 to modelOrder){
      noiseVariance :+= - coeffMatrices(i - 1) * crossCovMatrices(modelOrder - i)
    }

    (coeffMatrices, noiseVariance)
  }

  override def estimate(slice: Array[(IndexT, DenseVector[Double])]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val (crossCovMatrices, covMatrix) = super.estimate(slice)
    estimateVARMatrices(crossCovMatrices, covMatrix)

  }

  override def estimate(timeSeries: SingleAxisBlock[IndexT, DenseVector[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val (crossCovMatrices, covMatrix) = super.estimate(timeSeries)
    estimateVARMatrices(crossCovMatrices, covMatrix)

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): (Array[DenseMatrix[Double]], DenseMatrix[Double])= {

    val (crossCovMatrices, covMatrix) = super.estimate(timeSeries)
    estimateVARMatrices(crossCovMatrices, covMatrix)

  }

}
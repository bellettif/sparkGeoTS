package overlapping.timeSeries.secondOrder.multivariate

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import overlapping.containers.SingleAxisBlock
import overlapping.timeSeries.secondOrder.multivariate.frequentistEstimators.procedures.RybickiMulti

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
class VARModel[IndexT <: Ordered[IndexT] : ClassTag](
    deltaT: Double,
    p: Int,
    d: Int,
    mean: Broadcast[DenseVector[Double]]
  )
  extends CrossCovariance[IndexT](deltaT, p, d, mean){

  def estimateVARMatrices(crossCovMatrices: Array[DenseMatrix[Double]], covMatrix: DenseMatrix[Double]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) ={
    val nCols = covMatrix.rows

    val coeffMatrices = RybickiMulti(p, nCols,
      crossCovMatrices.slice(1, 2 * p),
      crossCovMatrices.slice(p + 1, 2 * p + 1))

    coeffMatrices.foreach(x => x := x.t)

    var noiseVariance = covMatrix
    for(i <- 1 to p){
      noiseVariance :+= - coeffMatrices(i - 1) * crossCovMatrices(p - i)
    }

    (coeffMatrices, noiseVariance)
  }

  override def windowEstimate(slice: Array[(IndexT, DenseVector[Double])]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val (crossCovMatrices, covMatrix) = super.windowEstimate(slice)
    estimateVARMatrices(crossCovMatrices, covMatrix)

  }

  override def blockEstimate(block: SingleAxisBlock[IndexT, DenseVector[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val (crossCovMatrices, covMatrix) = super.blockEstimate(block)
    estimateVARMatrices(crossCovMatrices, covMatrix)

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): (Array[DenseMatrix[Double]], DenseMatrix[Double])= {

    val (crossCovMatrices, covMatrix) = super.estimate(timeSeries)
    estimateVARMatrices(crossCovMatrices, covMatrix)

  }

}
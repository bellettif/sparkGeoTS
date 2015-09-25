package overlapping.models.secondOrder.multivariate.frequentistEstimators

import breeze.linalg._
import org.apache.spark.rdd.RDD
import overlapping.containers.block.SingleAxisBlock
import overlapping.models.secondOrder.multivariate.frequentistEstimators.procedures.{InnovationAlgoMulti, RybickiMulti}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/14/15.
 */
class VARMAModel[IndexT <: Ordered[IndexT] : ClassTag](deltaT: Double, p: Int, q: Int)
  extends CrossCovariance[IndexT](deltaT, p + q){

  /*
  Check out Brockwell, Davis, Time Series: Theory and Methods, 1987 (p 243)
   */
  def getMACoefs(psiCoeffs: Array[DenseMatrix[Double]], coeffsAR: Array[DenseMatrix[Double]]): Array[DenseMatrix[Double]] ={

    val d = psiCoeffs(0).rows

    val MACoefs = Array.fill(q){DenseMatrix.zeros[Double](d, d)}

    for(j <- 0 until q){
      MACoefs(j) = psiCoeffs(j)
      for(i <- 1 until (j min p)){
        MACoefs(j) -= coeffsAR(i - 1) * psiCoeffs(j - i)
      }
      if(p >= j){
        MACoefs(j) -= coeffsAR(j)
      }
    }
    MACoefs
  }

  /*
  TODO: there is an issue here whenever most pre-estimation thetas are zero. Need to use another estimation procedure.
   */
  def computeARMACoeffs(crossCovMatrices: Array[DenseMatrix[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val (psiCoeffs, noiseVariance) = InnovationAlgoMulti(p + q, crossCovMatrices)

    val d = psiCoeffs(0).rows

    val coeffsAR = RybickiMulti(p, d,
      psiCoeffs.slice(q - p, q + p - 1).map(_.t),
      psiCoeffs.slice(q, q + p).map(_.t))
    .map(_.t)

    val coeffsMA = getMACoefs(psiCoeffs, coeffsAR)

    val coeffMatrices: Array[DenseMatrix[Double]] = coeffsAR ++ coeffsMA

    (coeffMatrices, noiseVariance)

  }

  /*
  override def estimate(slice: Array[(IndexT, DenseVector[Double])]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    computeARMACoeffs(super.estimate(slice)._1)

  }

  override def estimate(timeSeries: SingleAxisBlock[IndexT, DenseVector[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    computeARMACoeffs(super.estimate(timeSeries)._1)

  }
  */

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    computeARMACoeffs(super.estimate(timeSeries)._1)

  }

}
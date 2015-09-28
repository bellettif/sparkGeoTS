package overlapping.models.secondOrder.univariate

import breeze.linalg._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import overlapping.containers.block.SingleAxisBlock
import overlapping.models.Predictor
import overlapping.models.secondOrder.univariate.procedures.{InnovationAlgo, Rybicki}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/14/15.
 */
class ARMAModel[IndexT <: Ordered[IndexT] : ClassTag](
  deltaT: Double,
  p: Int,
  q: Int,
  d: Int,
  mean: Broadcast[DenseVector[Double]])
  extends AutoCovariances[IndexT](deltaT, p + q, d, mean){

  /*
  Check out Brockwell, Davis, Time Series: Theory and Methods, 1987 (p 243)
   */
  def getMACoefs(psiCoeffs: DenseVector[Double], aCoeffs: DenseVector[Double]): DenseVector[Double] ={

    val MACoefs = DenseVector.zeros[Double](q)

    for(j <- 0 until q){
      MACoefs(j) = psiCoeffs(j)
      for(i <- 1 until (j min p)){
        MACoefs(j) -= aCoeffs(i - 1) * psiCoeffs(j - i)
      }
      if(p >= j){
        MACoefs(j) -= aCoeffs(j)
      }
    }

    MACoefs
  }

  /*
  TODO: there is an issue here whenever most pre-estimation thetas are zero. Need to use another estimation procedure.
   */
  def computeARMACoeffs(autoCovs: CovSignature): CovSignature = {

    val signaturePQ = InnovationAlgo(p + q, autoCovs.covariation)

    val coeffsAR: DenseVector[Double] = Rybicki(
      p,
      signaturePQ.covariation(q - p to q + p - 2),
      signaturePQ.covariation(q to q + p - 1))

    val coeffsMA: DenseVector[Double] = getMACoefs(signaturePQ.covariation, coeffsAR)

    val coeffs: DenseVector[Double] = DenseVector.vertcat(coeffsAR, coeffsMA)

    CovSignature(coeffs, signaturePQ.variation)

  }

  override def windowEstimate(slice: Array[(IndexT, DenseVector[Double])]): Array[CovSignature] = {

    super
      .windowEstimate(slice)
      .map(computeARMACoeffs)

  }

  override def blockEstimate(block: SingleAxisBlock[IndexT, DenseVector[Double]]): Array[CovSignature] = {

    super
      .blockEstimate(block)
      .map(computeARMACoeffs)

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): Array[CovSignature]= {

    super
      .estimate(timeSeries)
      .map(computeARMACoeffs)

  }

}
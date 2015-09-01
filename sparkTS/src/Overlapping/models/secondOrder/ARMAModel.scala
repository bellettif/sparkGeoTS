package overlapping.models.secondOrder

import breeze.linalg._
import org.apache.spark.rdd.RDD
import overlapping.containers.block.SingleAxisBlock
import overlapping.models.secondOrder.procedures.{InnovationAlgo, Rybicki}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/14/15.
 */
class ARMAModel[IndexT <: Ordered[IndexT] : ClassTag](deltaT: Double, p: Int, q: Int)
  extends AutoCovariances[IndexT](deltaT, p + q){

  /*
  Check out Brockwell, Davis, Time Series: Theory and Methods, 1987 (p 243)
   */
  def getMACoefs(pqEstTheta: DenseVector[Double], pqEstPhis: DenseVector[Double]):
  DenseVector[Double] ={
    val MACoefs = DenseVector.zeros[Double](q)
    for(j <- 0 until q){
      MACoefs(j) = pqEstTheta(j)
      for(i <- 1 to (j min p)){
        MACoefs(j) -= pqEstPhis(i - 1) * pqEstTheta(j - i)
      }
      if(p >= j){
        MACoefs(j) -= pqEstPhis(j)
      }
    }
    MACoefs
  }

  /*
  private[this] def getVar(pqEstTheta: DenseVector[Double], autoCov: DenseVector[Double]): Double ={
    autoCov(0) - sum(pqEstTheta :* pqEstTheta :* rev)
  }
  */

  /*
  TODO: there is an issue here whenever most pre-estimation thetas are zero. Need to use another estimation procedure.
   */
  def computeARMACoeffs(autoCovs: Signature): Signature = {

    val signaturePQ = InnovationAlgo(p + q, autoCovs.covariation)

    val phis: DenseVector[Double] = Rybicki(
      p,
      signaturePQ.covariation(q - p to q + p - 2),
      signaturePQ.covariation(q to q + p - 1))

    val thetas: DenseVector[Double] = getMACoefs(signaturePQ.covariation, phis)

    val coeffs: DenseVector[Double] = DenseVector.vertcat(phis, thetas)

    Signature(coeffs, signaturePQ.variation)

  }

  override def estimate(slice: Array[(IndexT, Array[Double])]): Array[Signature] = {

    super
      .estimate(slice)
      .map(computeARMACoeffs)

  }

  override def estimate(timeSeries: SingleAxisBlock[IndexT, Array[Double]]): Array[Signature] = {

    super
      .estimate(timeSeries)
      .map(computeARMACoeffs)

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, Array[Double]])]): Array[Signature]= {

    super
      .estimate(timeSeries)
      .map(computeARMACoeffs)

  }

}
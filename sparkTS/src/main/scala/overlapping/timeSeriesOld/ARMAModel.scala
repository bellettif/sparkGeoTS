package main.scala.overlapping.timeSeriesOld

import breeze.linalg._
import main.scala.overlapping.timeSeries.secondOrder.univariate.Procedures.{InnovationAlgo, Toeplitz}
import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag


/**
 * Will return AR coeffs followed by MA coeffs.
 */
object ARMAModel{

  def apply[IndexT : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      p: Int,
      q: Int,
      mean: Option[DenseVector[Double]] = None): Array[SecondOrderSignature] = {

    val estimator = new ARMAModel[IndexT](
      p, q,
      timeSeries.config,
      timeSeries.content.context.broadcast(mean))

    estimator.estimate(timeSeries)

  }

}

class ARMAModel[IndexT : ClassTag](
    p: Int,
    q: Int,
    config: VectTSConfig[IndexT],
    mean: Broadcast[Option[DenseVector[Double]]])
  extends AutoCovariances[IndexT](p + q, config, mean){

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
  def computeARMACoeffs(autoCovs: SecondOrderSignature): SecondOrderSignature = {

    val signaturePQ = InnovationAlgo(p + q, autoCovs.covariation)

    val coeffsAR: DenseVector[Double] = Toeplitz(
      p,
      signaturePQ.covariation(q - p to q + p - 2),
      signaturePQ.covariation(q to q + p - 1))

    val coeffsMA: DenseVector[Double] = getMACoefs(signaturePQ.covariation, coeffsAR)

    val coeffs: DenseVector[Double] = DenseVector.vertcat(coeffsAR, coeffsMA)

    SecondOrderSignature(coeffs, signaturePQ.variation)

  }

  override def estimate(timeSeries: VectTimeSeries[IndexT]): Array[SecondOrderSignature]= {

      super
      .estimate(timeSeries)
      .map(computeARMACoeffs)

  }

}
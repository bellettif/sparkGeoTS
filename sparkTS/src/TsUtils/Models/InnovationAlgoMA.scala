package TsUtils.Models

import TsUtils.TimeSeries
import breeze.linalg._

/**
 * Created by Francois Belletti on 7/13/15.
 */
class InnovationAlgoMA(h: Int)
  extends AutoCovariance(h){

  /*

  This calibrate one univariate AR model per columns.
  Returns an array of calibrated parameters (Coeffs, variance of noise).

  Check out Brockwell, Davis, Time Series: Theory and Methods, 1987 (p 238)
  TODO: shield procedure against the following edge cases, autoCov.size < 1, autoCov(0) = 0.0
   */
  private [this] def proceed(autoCov: DenseVector[Double]): (DenseVector[Double], Double) ={
    val thetaEsts = (1 to h).toArray.map(DenseVector.zeros[Double](_))
    val varEsts   = DenseVector.zeros[Double](h + 1)

    varEsts(0)    = autoCov(0) // Potential edge case here whenever varEsts(0) == 0

    for(m <- 1 to h){
      for(k <- 0 until m){
        // In the book the theta estimate vector is filled from the tail to the head.
        // Here it is filled from the head to the tail.
        thetaEsts(m - 1)(k) = (autoCov(m - k) - sum(thetaEsts(m - 1)(0 until k) :* thetaEsts(k) :* varEsts(0 until k))) / varEsts(k)
      }
      varEsts(m) = autoCov(0) - sum(thetaEsts(m - 1) :* thetaEsts(m - 1) :* varEsts(0 until m))
    }

    // Reverse the result so as to have the same convention as in the book
    (reverse(thetaEsts(h - 1)), varEsts(h))
  }

  override def estimate(timeSeries: TimeSeries[_, Double]): Array[(DenseVector[Double], Double)] = {
    val autoCovs = super.estimate(timeSeries)
    autoCovs.asInstanceOf[Array[DenseVector[Double]]].map(proceed)
  }

  override def estimate(timeSeriesTile: Seq[Array[Double]]): Array[(DenseVector[Double], Double)] = {
    val autoCovs = super.estimate(timeSeriesTile)
    autoCovs.asInstanceOf[Array[DenseVector[Double]]].map(proceed)
  }


}

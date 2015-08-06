package TimeIndex.Estimators.RegularSpacing.Models

import TimeIndex.Containers.TimeSeries
import TimeIndex.Estimators.RegularSpacing.Procedures.{InnovationAlgo, Rybicki}
import breeze.linalg._

/**
 * Created by Francois Belletti on 7/14/15.
 */
class ARMAModel(p: Int, q: Int)
  extends AutoCovariance(p + q) with InnovationAlgo with Rybicki{

  /*
  Check out Brockwell, Davis, Time Series: Theory and Methods, 1987 (p 243)
   */
  private[this] def getMACoefs(pqEstTheta: DenseVector[Double], pqEstPhis: DenseVector[Double]):
  DenseVector[Double] ={
    val MACoefs = DenseVector.zeros[Double](q)
    for(j <- 0 until q){
      MACoefs(j) = pqEstTheta(j)
      for(i <- 1 to (j min p)){
        MACoefs(j) -= pqEstPhis(i - 1) * pqEstTheta(j - i)
      }
      if(p > j){
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
  TODO: there is an issue here whenever most pre-estimation thetas are zero. Need to use another calibration procedure.
   */
  override def estimate(timeSeries: TimeSeries[Double]): Array[(DenseVector[Double], DenseVector[Double])] = {
    val autoCovs = super.estimate(timeSeries)
    val thetasPQ = autoCovs.asInstanceOf[Array[DenseVector[Double]]]
      .map(x => runIA(p + q, x)._1)
    val phis: Array[DenseVector[Double]] = thetasPQ
      .map(x => runR(p, x(q - p to q + p - 2), x(q to q + p - 1)))
    val thetas = (thetasPQ zip phis).map({case (x, y) => getMACoefs(x, y)})
    phis zip thetas
  }

  override def estimate(timeSeriesTile: Array[Array[Double]]): Array[(DenseVector[Double], DenseVector[Double])] = {
    val autoCovs = super.estimate(timeSeriesTile)
    val thetasPQ = autoCovs.asInstanceOf[Array[DenseVector[Double]]]
      .map(x => runIA(p + q, x)._1)
    val phis: Array[DenseVector[Double]] = thetasPQ
      .map(x => runR(p, x(q - p to q + p - 2), x(q to q + p - 1)))
    val thetas = (thetasPQ zip phis).map({case (x, y) => getMACoefs(x, y)})
    phis zip thetas
  }


}

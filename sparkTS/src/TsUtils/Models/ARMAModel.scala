package TsUtils.Models

import TsUtils.Procedures.{InnovationAlgo, Rybicki}
import TsUtils.TimeSeries
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
      for(i <- 0 until (j min p)){
        MACoefs(j) -= pqEstPhis(i) * pqEstTheta(j - i)
      }
    }
    MACoefs
  }

  /*
  private[this] def getVar(pqEstTheta: DenseVector[Double], autoCov: DenseVector[Double]): Double ={
    autoCov(0) - sum(pqEstTheta :* pqEstTheta :* rev)
  }
  */

  override def estimate(timeSeries: TimeSeries[_, Double]): Array[(DenseVector[Double], DenseVector[Double])] = {
    val autoCovs = super.estimate(timeSeries)
    val thetasPQ = autoCovs.asInstanceOf[Array[DenseVector[Double]]]
      .map(x => runIA(p + q, x)._1)
    val temp1 = thetasPQ(0)
    val temp = runR(p, temp1(q - p to q + p - 2), temp1(q to q + p - 1))
    val phis: Array[DenseVector[Double]] = thetasPQ
      .map(x => runR(p, x(q - p to q + p - 2), x(q to q + p - 1)))
    val thetas = (thetasPQ zip phis).map({case (x, y) => getMACoefs(x, y)})
    phis zip thetas
  }

  override def estimate(timeSeriesTile: Seq[Array[Double]]): Array[(DenseVector[Double], DenseVector[Double])] = {
    val autoCovs = super.estimate(timeSeriesTile)
    val thetasPQ = autoCovs.asInstanceOf[Array[DenseVector[Double]]]
      .map(x => runIA(p + q, x)._1)
    val phis: Array[DenseVector[Double]] = thetasPQ
      .map(x => runR(p, x(q - (p - 1) to q + (p - 1)), x(q + 1 to q + p)))
    val thetas = (thetasPQ zip phis).map({case (x, y) => getMACoefs(x, y)})
    phis zip thetas
  }


}

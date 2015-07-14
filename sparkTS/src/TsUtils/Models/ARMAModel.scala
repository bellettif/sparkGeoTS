package TsUtils.Models

import TsUtils.Procedures.InnovationAlgo
import TsUtils.TimeSeries
import breeze.linalg._

/**
 * Created by Francois Belletti on 7/14/15.
 */
class ARMAModel(p: Int, q: Int)
  extends AutoCovariance(p + q) with InnovationAlgo{

  /*
  Check out Brockwell, Davis, Time Series: Theory and Methods, 1987 (p 243)
   */
  override def estimate(timeSeries: TimeSeries[_, Double]): Array[(DenseVector[Double], Double)] = {
    val autoCovs = super.estimate(timeSeries)
    val thetas = autoCovs.asInstanceOf[Array[DenseVector[Double]]].map(x => runIA(p + q, x))
    thetas
  }

  /*
  Check out Brockwell, Davis, Time Series: Theory and Methods, 1987 (p 243)
   */
  override def estimate(timeSeriesTile: Seq[Array[Double]]): Array[(DenseVector[Double], Double)] = {
    val autoCovs = super.estimate(timeSeriesTile)
    val thetas = autoCovs.asInstanceOf[Array[DenseVector[Double]]].map(x => runIA(p + q, x))
    thetas
  }


}

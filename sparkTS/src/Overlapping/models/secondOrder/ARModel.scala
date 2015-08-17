package overlapping.models.secondOrder

/*

import breeze.linalg._
import timeIndex.containers.TimeSeries
import overlapping.models.secondOrder.procedures.DurbinLevinson

/**
 * Created by Francois Belletti on 7/13/15.
 */
class ARModel(p: Int)
  extends AutoCovariance(p) with DurbinLevinson{

  override def estimate(timeSeries: TimeSeries[Double]): Array[(DenseVector[Double], Double)] = {
    val autoCovs = super.estimate(timeSeries)
    autoCovs.asInstanceOf[Array[DenseVector[Double]]].map(x => runDL(p, x))
  }

  override def estimate(timeSeriesTile: Array[Array[Double]]): Array[(DenseVector[Double], Double)] = {
    val autoCovs = super.estimate(timeSeriesTile)
    autoCovs.asInstanceOf[Array[DenseVector[Double]]].map(x => runDL(p, x))
  }


}


*/
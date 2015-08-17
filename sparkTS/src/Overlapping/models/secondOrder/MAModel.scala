package overlapping.models.secondOrder

/*

import breeze.linalg._
import timeIndex.containers.TimeSeries
import overlapping.models.secondOrder.procedures.InnovationAlgo

/**
 * Created by Francois Belletti on 7/13/15.
 */
class MAModel(q: Int)
  extends AutoCovariance(q) with InnovationAlgo{

  override def estimate(timeSeries: TimeSeries[Double]): Array[(DenseVector[Double], Double)] = {
    val autoCovs = super.estimate(timeSeries)
    autoCovs.asInstanceOf[Array[DenseVector[Double]]].map(x => runIA(q, x))
  }

  override def estimate(timeSeriesTile: Array[Array[Double]]): Array[(DenseVector[Double], Double)] = {
    val autoCovs = super.estimate(timeSeriesTile)
    autoCovs.asInstanceOf[Array[DenseVector[Double]]].map(x => runIA(q, x))
  }


}

*/
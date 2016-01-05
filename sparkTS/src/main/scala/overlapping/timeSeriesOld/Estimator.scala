package main.scala.overlapping.timeSeriesOld

/**
 * Created by Francois Belletti on 9/24/15.
 */
trait Estimator[IndexT, EstimateT]
  extends Serializable{

  def estimate(timeSeries: VectTimeSeries[IndexT]): EstimateT

}

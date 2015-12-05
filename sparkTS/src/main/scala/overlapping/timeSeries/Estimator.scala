package main.scala.overlapping.timeSeries

import main.scala.overlapping.containers.TimeSeries

/**
 * Created by Francois Belletti on 9/24/15.
 */
trait Estimator[IndexT <: Ordered[IndexT], ValueT, EstimateT]
  extends Serializable{

  def estimate(timeSeries: TimeSeries[IndexT, ValueT]): EstimateT

}

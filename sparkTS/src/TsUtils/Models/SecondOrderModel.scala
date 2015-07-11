package TsUtils.Models

import TsUtils.TimeSeries

/**
 * Created by Francois Belletti on 7/10/15.
 */
abstract trait SecondOrderModel
  extends Serializable{

  def estimate(timeSeries: TimeSeries[_, _]) = ???

}

package TsUtils.Models

import TsUtils.TimeSeries

/**
 * Created by Francois Belletti on 7/10/15.
 */
abstract trait SecondOrderModel[DataType]
  extends Serializable{

  def estimate(timeSeries: TimeSeries[DataType]): Any = ???

  def estimate(timeSeriesTile: Array[Array[DataType]]): Any = ???

}

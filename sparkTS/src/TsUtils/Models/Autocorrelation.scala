package TsUtils.Models

import TsUtils.TimeSeries

import scala.reflect._
import breeze.linalg._

/**
 * Created by Francois Belletti on 7/10/15.
 */
class Autocorrelation(h: Int)
  extends Serializable with SecondOrderModel{

  def estimate(timeSeries: TimeSeries[_, Double]): Array[DenseVector[Double]]={

    val nCols = timeSeries.nColumns

    val result = (0 until nCols).toArray.map(x => DenseVector.zeros[Double](h + 1))

    for(i <- 0 until nCols){
      result(i)(0) = 1.0 // Rank zero autocorrelation
      for(lag <- 1 to h){
        result(i)(lag) = timeSeries.computeCrossFold[Double](_*_, _+_, i, i, lag, 0.0) / timeSeries.nSamples.value
      }
    }

    result
  }

}

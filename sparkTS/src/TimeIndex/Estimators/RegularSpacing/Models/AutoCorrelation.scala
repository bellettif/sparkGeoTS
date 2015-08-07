package timeIndex.estimators.regularSpacing.models


import timeIndex.containers.TimeSeries

import scala.reflect._
import breeze.linalg._

/**
 * Created by Francois Belletti on 7/10/15.
 */
class AutoCorrelation(h: Int)
  extends Serializable with SecondOrderModel[Double]{

  override def estimate(timeSeries: TimeSeries[Double]): Array[DenseVector[Double]]={

    val nCols = timeSeries.config.nCols

    val result = (0 until nCols.value).toArray.map(x => DenseVector.zeros[Double](h + 1))

    timeSeries.dataTiles.persist()

    for(i <- 0 until nCols.value){
      for(lag <- 0 to h){
        result(i)(lag) = timeSeries.computeCrossFold[Double](_*_, _+_, i, i, lag, 0.0)
      }
      if(result(i)(0) != 0.0) {
        val variance = result(i)(0)
        result(i)(0) = 1.0
        for (lag <- 1 to h) {
          result(i)(lag) /= variance
        }
      }
    }

    timeSeries.dataTiles.unpersist()

    result
  }

  private[this] def autoCov(univSeries: Array[Double], lag: Int): Double ={
    var res: Double = 0.0
    for(i <- 0 until univSeries.size - lag){
      res += univSeries.apply(i + lag) * univSeries.apply(i)
    }
    res
  }

  override def estimate(timeSeriesTile: Array[Array[Double]]): Array[DenseVector[Double]] ={

    val nCols = timeSeriesTile.size

    val result = (0 until nCols).toArray.map(x => DenseVector.zeros[Double](h + 1))

    if(timeSeriesTile.isEmpty) return result

    val nSamples: Int = timeSeriesTile.size

    if(nSamples == 0) return result

    for(i <- 0 until nCols){
      for(lag <- 0 to h){
        result(i)(lag) = autoCov(timeSeriesTile(i), lag)
      }
      if(result(i)(0) != 0.0) {
        val variance = result(i)(0)
        result(i)(0) = 1.0
        for (lag <- 1 to h) {
          result(i)(lag) /= variance
        }
      }
    }
    result

  }



}

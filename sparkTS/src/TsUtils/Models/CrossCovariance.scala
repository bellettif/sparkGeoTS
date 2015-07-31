package TsUtils.Models

import TsUtils.TimeSeries
import breeze.linalg._

/**
 * Created by Francois Belletti on 7/10/15.
 */
class CrossCovariance(h: Int)
  extends Serializable with SecondOrderModel[Double]{

  override def estimate(timeSeries: TimeSeries[Double]): Array[DenseMatrix[Double]]={

    timeSeries.dataTiles.persist()

    val nCols     = timeSeries.config.nCols
    val nSamples  = timeSeries.config.nSamples

    val result = (0 until nCols.value).toArray.map(x => DenseMatrix.zeros[Double](nCols.value, h + 1))

    for(i <- 0 until nCols.value){
      for(j <- 0 until nCols.value){
        for (lag <- 0 to h){
          result(i)(j, lag) = timeSeries.computeCrossFold[Double](_ * _, _ + _, i, j, lag, 0.0) / nSamples.value
        }
      }
    }

    timeSeries.dataTiles.unpersist()

    result
  }

  /*
  Issue here: if lag is too high the result will be zero and not NA.
   */
  private[this] def crossCov(leftCol: Array[Double], rightCol: Array[Double], lag: Int): Double ={
    var res: Double = 0.0
    for(i <- 0 until leftCol.length - lag){
      res += leftCol(i + lag) * rightCol(i)
    }
    res
  }

  override def estimate(timeSeriesTile: Array[Array[Double]]): Array[DenseMatrix[Double]] ={

    val nCols = timeSeriesTile.size

    val result = (0 until nCols).toArray.map(x => DenseMatrix.zeros[Double](nCols, h + 1))

    if(timeSeriesTile.isEmpty) return result

    val nSamples: Int = timeSeriesTile.size

    if(nSamples == 0) return result

    for(i <- 0 until nCols){
      for(j <- 0 until nCols){
        for (lag <- 0 to h){
          result(i)(j, lag) = crossCov(timeSeriesTile(i), timeSeriesTile(j), lag) / nSamples.toDouble
        }
      }
    }
    result

  }



}

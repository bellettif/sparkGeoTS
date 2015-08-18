package overlapping.models.secondOrder

import breeze.linalg._
import overlapping.IntervalSize
import overlapping.containers.block.SingleAxisBlock

/**
 * Created by Francois Belletti on 7/10/15.
 */

/*
Here we expect the number of dimensions to be the same for all records.
 */
class CrossCovariance[IndexT <: Ordered[IndexT]](selectionSize: IntervalSize, modelOrder: Int)
  extends Serializable with SecondOrderModel[IndexT, Array[Double]]{

  def computeCrossCov(accumulator: Array[DenseMatrix[Double]])
                     (slice: Array[(IndexT, Array[Double])]): Unit = {

    if(slice.length != 2 * modelOrder + 1){
      return
    }

    val nCols         = slice(0)._2.length
    val centerTarget  = slice(modelOrder)._2

    for(i <- 0 until 2 * modelOrder + 1){
      val currentTarget = slice(i)._2
      for(c1 <- 0 until nCols){
        for(c2 <- 0 until nCols){
          accumulator(i)(c1, c2) += centerTarget(c1) * currentTarget(c2)
        }
      }
    }

  }


  override def estimate(timeSeries: SingleAxisBlock[IndexT, Array[Double]]): Array[DenseMatrix[Double]]={

    val nCols = timeSeries.take(1).head._2.length

    val result = Array.fill(2 * modelOrder + 1)(DenseMatrix.zeros[Double](nCols, nCols))

    timeSeries.sliding(Array(selectionSize))(computeCrossCov(result)(_))

    result

  }

}
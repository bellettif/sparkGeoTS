package overlapping.models.secondOrder


import breeze.linalg._
import org.apache.spark.rdd.RDD
import overlapping.IntervalSize
import overlapping.containers.block.{SingleAxisBlock, ColumnFirstBlock}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/10/15.
 */

/*
  Compute individual covariance functions
 */
class AutoCovariances[IndexT <: Ordered[IndexT] : ClassTag](lookBack: Double, modelOrder: Int)
  extends Serializable with SecondOrderModel[IndexT, Array[Double]]{

  def computeCovariationKernel(indices: Array[IndexT], data: Array[Double]): (Signature, Long) = {

    if(indices.length != modelOrder + 1){
      return (Signature(DenseVector.zeros(modelOrder + 1), 0.0), 0L)
    }

    val centerTarget = data(modelOrder)

    (Signature(DenseVector(data) :* centerTarget, centerTarget * centerTarget), 1L)
  }

  def sumSignatures(x: (Signature, Long), y: (Signature, Long)): (Signature, Long) = {
    (Signature(x._1.covariation :+ y._1.covariation, x._1.variation + y._1.variation), x._2 + y._2)
  }

  def sumSignatureArrays(x: Array[(Signature, Long)], y: Array[(Signature, Long)]): Array[(Signature, Long)] = {
    x.zip(y).map({case (u, v) => sumSignatures(u, v)})
  }

  def computeCovariation(timeSeries: SingleAxisBlock[IndexT, Array[Double]]): Array[(Signature, Long)]= {
    val temp = new ColumnFirstBlock[IndexT](timeSeries.data, timeSeries.locations, timeSeries.signedDistances)

    val zeros: Array[(Signature, Long)] = Array.fill(temp.nCols)((Signature(DenseVector.zeros[Double](modelOrder + 1), 0.0), 0L))
    val kernels: Array[(Array[IndexT], Array[Double]) => (Signature, Long)] = Array.fill(temp.nCols)(computeCovariationKernel)
    val ops: Array[((Signature, Long), (Signature, Long)) => (Signature, Long)] = Array.fill(temp.nCols)(sumSignatures)

    val selectionSize = IntervalSize(lookBack, 0)

    temp.columnSlidingFold(Array(selectionSize))(kernels, zeros, ops)
  }

  def normalize = (r: (Signature, Long)) => Signature(reverse(r._1.covariation) / r._2.toDouble, r._1.variation / r._2.toDouble)

  override def estimate(slice: Array[(IndexT, Array[Double])]): Array[Signature] = {

    val columns = slice
      .map(_._2)
      .transpose
      .map(x => slice.map(_._1).sliding(modelOrder + 1).zip(x.sliding(modelOrder + 1)))

    columns.map(_.map({case (x, y) => computeCovariationKernel(x, y)})
      .reduce(sumSignatures))
      .map(normalize)

  }

  override def estimate(timeSeries: SingleAxisBlock[IndexT, Array[Double]]): Array[Signature]= {

    computeCovariation(timeSeries)
      .map(normalize)

  }


  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, Array[Double]])]): Array[Signature]={

    timeSeries
      .mapValues(computeCovariation)
      .map(_._2)
      .reduce(sumSignatureArrays)
      .map(normalize)

  }

}

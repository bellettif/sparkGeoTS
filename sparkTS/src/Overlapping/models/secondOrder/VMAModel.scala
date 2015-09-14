package overlapping.models.secondOrder

import breeze.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import overlapping.containers.block.SingleAxisBlock
import overlapping.models.secondOrder.procedures.{InnovationAlgoMulti}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
class VMAModel[IndexT <: Ordered[IndexT] : ClassTag](deltaT: Double, modelOrder: Int)
  extends CrossCovariance[IndexT](deltaT, modelOrder){

  def estimateVMAMatrices(crossCovMatrices: Array[DenseMatrix[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) ={

    InnovationAlgoMulti(modelOrder, crossCovMatrices)

  }

  override def estimate(slice: Array[(IndexT, Array[Double])]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val crossCovMatrices: Array[DenseMatrix[Double]] = super.estimate(slice)._1
    estimateVMAMatrices(crossCovMatrices)

  }

  override def estimate(timeSeries: SingleAxisBlock[IndexT, Array[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val crossCovMatrices: Array[DenseMatrix[Double]] = super.estimate(timeSeries)._1
    estimateVMAMatrices(crossCovMatrices)

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, Array[Double]])]): (Array[DenseMatrix[Double]], DenseMatrix[Double])= {

    val crossCovMatrices: Array[DenseMatrix[Double]] = super.estimate(timeSeries)._1
    estimateVMAMatrices(crossCovMatrices)

  }


}
package main.scala.overlapping.timeSeriesOld

import breeze.linalg._
import main.scala.overlapping.timeSeriesOld.secondOrder.multivariate.frequentistEstimators.procedures.{InnovationAlgoMulti, ToeplitzMulti}
import main.scala.overlapping.timeSeriesOld.CrossCovariance
import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/14/15.
 */
object VARMAModel{

  def apply[IndexT : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      p: Int,
      q: Int,
      mean: Option[DenseVector[Double]] = None): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val estimator = new VARMAModel[IndexT](p, q, timeSeries.config, timeSeries.content.context.broadcast(mean))
    estimator.estimate(timeSeries)

  }

}


class VARMAModel[IndexT : ClassTag](
    p: Int,
    q: Int,
    config: VectTSConfig[IndexT],
    mean: Broadcast[Option[DenseVector[Double]]])
  extends CrossCovariance[IndexT](p + q, config, mean){

  def getMACoefs(psiCoeffs: Array[DenseMatrix[Double]], coeffsAR: Array[DenseMatrix[Double]]): Array[DenseMatrix[Double]] ={

    val d = psiCoeffs(0).rows

    val MACoefs = Array.fill(q){DenseMatrix.zeros[Double](d, d)}

    for(j <- 0 until q){
      MACoefs(j) = psiCoeffs(j)
      for(i <- 1 until (j min p)){
        MACoefs(j) -= coeffsAR(i - 1) * psiCoeffs(j - i)
      }
      if(p >= j){
        MACoefs(j) -= coeffsAR(j)
      }
    }
    MACoefs

  }

  /*
  TODO: there is an issue here whenever most pre-estimation thetas are zero. Need to use another estimation procedure.
   */
  def computeARMACoeffs(crossCovMatrices: Array[DenseMatrix[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val (psiCoeffs, noiseVariance) = InnovationAlgoMulti(p + q, crossCovMatrices)

    val d = psiCoeffs(0).rows

    val coeffsAR = ToeplitzMulti(p, d,
      psiCoeffs.slice(q - p, q + p - 1).map(_.t),
      psiCoeffs.slice(q, q + p).map(_.t))
    .map(_.t)

    val coeffsMA = getMACoefs(psiCoeffs, coeffsAR)

    val coeffMatrices: Array[DenseMatrix[Double]] = coeffsAR ++ coeffsMA

    (coeffMatrices, noiseVariance)

  }

  override def estimate(timeSeries: VectTimeSeries[IndexT]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    computeARMACoeffs(super.estimate(timeSeries)._1)

  }

}
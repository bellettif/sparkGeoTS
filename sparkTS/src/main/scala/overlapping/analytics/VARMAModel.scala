package main.scala.overlapping.analytics

import breeze.linalg._
import main.scala.overlapping.containers.{TSInstant, SingleAxisVectTS}
import main.scala.procedures.{ToeplitzMulti, InnovationAlgoMulti}
import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/14/15.
 */
object VARMAModel{

  def getMACoefs(
      p: Int,
      q: Int,
      psiCoeffs: Array[DenseMatrix[Double]],
      coeffsAR: Array[DenseMatrix[Double]]): Array[DenseMatrix[Double]] ={

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


  def apply[IndexT : TSInstant : ClassTag](
    timeSeries: SingleAxisVectTS[IndexT],
    p: Int,
    q: Int,
    mean: Option[DenseVector[Double]] = None): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    /*
    TODO: there is an issue here whenever most pre-estimation thetas are zero. Need to use another estimation procedure.
    */

    val crossCovMatrices = CrossCovariance(timeSeries, p + q, mean)


    val (psiCoeffs, noiseVariance) = InnovationAlgoMulti(p + q, crossCovMatrices)

    val d = timeSeries.config.dim

    val coeffsAR = ToeplitzMulti(p, d,
      psiCoeffs.slice(q - p, q + p - 1).map(_.t),
      psiCoeffs.slice(q, q + p).map(_.t))
    .map(_.t)

    val coeffsMA = getMACoefs(p, q, psiCoeffs, coeffsAR)

    val coeffMatrices: Array[DenseMatrix[Double]] = coeffsAR ++ coeffsMA

    (coeffMatrices, noiseVariance)

  }

}
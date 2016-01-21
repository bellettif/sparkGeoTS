package main.scala.overlapping.analytics

import breeze.linalg.{DenseMatrix, DenseVector}
import main.scala.overlapping.containers.{SingleAxisVectTS, TSInstant}
import main.scala.procedures.ToeplitzMulti

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
object VARModel{

  /**
   * Estimate a multivariate autoregressive model or order p VAR(p)
   *
   * @param timeSeries
   * @param p
   * @param mean
   * @tparam IndexT
   * @return
   */
  def apply[IndexT : TSInstant : ClassTag](
      timeSeries: SingleAxisVectTS[IndexT],
      p: Int,
      mean: Option[DenseVector[Double]] = None): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val config = timeSeries.config
    val d = config.dim
    val deltaT = config.deltaT

    val bckPadding = implicitly[TSInstant[IndexT]].times(deltaT, p)
    if (implicitly[TSInstant[IndexT]].compare(bckPadding, config.bckPadding) > 0) {
      throw new IndexOutOfBoundsException("Not enough padding to support AR(p) estimation. At least deltaT * p is necessary.")
    }

    val crossCovMatrices = CrossCovariance(timeSeries, p, mean)

    val covMatrix = crossCovMatrices(p)

    val coeffMatrices = ToeplitzMulti(p, d,
      crossCovMatrices.slice(1, 2 * p),
      crossCovMatrices.slice(p + 1, 2 * p + 1))

    coeffMatrices.foreach(x => x := x.t)

    var noiseVariance = covMatrix
    for(i <- 1 to p){
      noiseVariance :+= - coeffMatrices(i - 1) * crossCovMatrices(p - i)
    }

    (coeffMatrices, noiseVariance)

  }

}
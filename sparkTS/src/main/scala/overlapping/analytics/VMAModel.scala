package main.scala.overlapping.analytics

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.numerics.sqrt
import main.scala.overlapping.containers.{TSInstant, SingleAxisVectTS}
import main.scala.procedures.InnovationAlgoMulti

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/13/15.
 */

object VMAModel{

  def apply[IndexT : TSInstant : ClassTag](
      timeSeries: SingleAxisVectTS[IndexT],
      q: Int,
      mean: Option[DenseVector[Double]] = None): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val config = timeSeries.config
    val d = config.dim
    val deltaT = config.deltaT

    val bckPadding = implicitly[TSInstant[IndexT]].times(deltaT, q)
    if (implicitly[TSInstant[IndexT]].compare(bckPadding, config.bckPadding) > 0) {
      throw new IndexOutOfBoundsException("Not enough padding to support MA(q) estimation. At least deltaT * q is necessary.")
    }

    InnovationAlgoMulti(q, CrossCovariance(timeSeries, q, mean))

  }

}
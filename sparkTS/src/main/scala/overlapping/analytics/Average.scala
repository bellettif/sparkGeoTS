package main.scala.overlapping.analytics

import breeze.linalg._
import main.scala.overlapping.containers._

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/10/15.
 */

/**
  Here we expect the number of dimensions to be the same for all records.
  */

object Average{

  def apply[IndexT : TSInstant : ClassTag](
      timeSeries: SingleAxisVectTS[IndexT]): DenseVector[Double] = {

    val config = timeSeries.config

    def zero = DenseVector.zeros[Double](config.dim)

    timeSeries.fold(zero)({case (x, y) => y}, _ + _) / config.nSamples.toDouble

  }

}


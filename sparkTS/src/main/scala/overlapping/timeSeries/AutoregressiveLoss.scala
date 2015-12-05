package main.scala.overlapping.timeSeries

import breeze.linalg.{DenseMatrix, DenseVector}
import main.scala.overlapping.containers.{TSInstant, VectTSConfig}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/24/15.
 */
class AutoregressiveLoss[IndexT <: TSInstant[IndexT] : ClassTag](
  p: Int,
  lossFunction: (Array[DenseMatrix[Double]], Array[(IndexT, DenseVector[Double])]) => Double,
  config: VectTSConfig[IndexT],
  dim: Option[Int] = None)
extends SecondOrderEssStat[IndexT, DenseVector[Double], Double]
{

  val d = dim.getOrElse(config.dim)
  val x = Array.fill(p){DenseMatrix.zeros[Double](d, d)}

  override def selection = config.selection

  def modelOrder = ModelSize(p, 0)

  def zero = 0.0

  def setNewX(newX: Array[DenseMatrix[Double]]) = {
    val maxEigenValue = Stability(newX)

    for(i <- x.indices){
      x(i) := newX(i) / maxEigenValue
    }
  }

  override def kernel(slice: Array[(IndexT, DenseVector[Double])]): Double = {

    if(slice.length != modelWidth){
      return 0.0
    }
    lossFunction(x, slice)

  }

  override def reducer(x: Double, y: Double): Double = {x + y}

}

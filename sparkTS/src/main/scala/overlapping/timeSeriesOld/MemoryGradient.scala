package main.scala.overlapping.timeSeriesOld

import breeze.linalg.{DenseMatrix, DenseVector}
import main.scala.overlapping.containers.TSInstant

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/24/15.
 */
class MemoryGradient[IndexT : ClassTag](
    q: Int,
    gradientFunction: (Array[DenseMatrix[Double]], Array[(TSInstant[IndexT], DenseVector[Double])], Array[DenseVector[Double]]) => (Array[DenseMatrix[Double]], Array[DenseVector[Double]]),
    config: VectTSConfig[IndexT],
    dim: Option[Int] = None)
extends SecondOrderEssStatMemory[IndexT, Array[DenseMatrix[Double]], Array[DenseVector[Double]]]
{

  val d = dim.getOrElse(config.dim)
  val x = Array.fill(q){DenseMatrix.zeros[Double](d, d)}

  val gradientSizes = x.map(y => (y.rows, y.cols))

  override def selection = config.selection

  override def modelOrder = ModelSize(0, 0)

  override def zero = gradientSizes.map({case (nRows, nCols) => DenseMatrix.zeros[Double](nRows, nCols)})

  def init = Array.fill(q){DenseVector.zeros[Double](d)}

  def setNewX(newX: Array[DenseMatrix[Double]]) = {
    val maxEigenValue = Stability(newX)

    for(i <- x.indices){
      x(i) := newX(i) / maxEigenValue
    }
  }

  def getGradientSize = gradientSizes

  override def kernel(slice: Array[(TSInstant[IndexT], DenseVector[Double])], state: Array[DenseVector[Double]]): (Array[DenseMatrix[Double]], Array[DenseVector[Double]]) = {

    if(slice.length != modelWidth){
      return (gradientSizes.map({case (r, c) => DenseMatrix.zeros[Double](r, c)}), gradientFunction(x, slice, state)._2)
    }

    gradientFunction(x, slice, state)

  }

  override def reducer(x: Array[DenseMatrix[Double]], y: Array[DenseMatrix[Double]]): Array[DenseMatrix[Double]] ={
    x.zip(y).map({case (x, y) => x + y})
  }

}

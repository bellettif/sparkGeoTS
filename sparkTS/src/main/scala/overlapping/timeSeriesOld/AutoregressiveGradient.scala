package main.scala.overlapping.timeSeriesOld

import breeze.linalg.{DenseMatrix, DenseVector}
import main.scala.overlapping.containers.TSInstant

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/24/15.
 */
class AutoregressiveGradient[IndexT : ClassTag](
    p: Int,
    gradientFunction: (Array[DenseMatrix[Double]], Array[(TSInstant[IndexT], DenseVector[Double])]) => Array[DenseMatrix[Double]],
    config: VectTSConfig[IndexT],
    dim: Option[Int] = None)
extends SecondOrderEssStat[IndexT, Array[DenseMatrix[Double]]]
{

  val d = dim.getOrElse(config.dim)
  val x = Array.fill(p){DenseMatrix.zeros[Double](d, d)}

  val gradientSizes = x.map(y => (y.rows, y.cols))

  override def selection = config.selection

  override def modelOrder = ModelSize(p, 0)

  override def zero = gradientSizes.map({case (nRows, nCols) => DenseMatrix.zeros[Double](nRows, nCols)})

  def setNewX(newX: Array[DenseMatrix[Double]]) = {
    val maxEigenValue = Stability(newX)

    for(i <- x.indices){
      x(i) := newX(i) / maxEigenValue
    }
  }

  def getGradientSize = gradientSizes

  override def kernel(slice: Array[(TSInstant[IndexT], DenseVector[Double])]): Array[DenseMatrix[Double]] = {

    if(slice.length != modelWidth){
      return gradientSizes.map({case (r, c) => DenseMatrix.zeros[Double](r, c)})
    }
    gradientFunction(x, slice)

  }

  override def reducer(x: Array[DenseMatrix[Double]], y: Array[DenseMatrix[Double]]): Array[DenseMatrix[Double]] ={
    x.zip(y).map({case (x, y) => x + y})
  }

}

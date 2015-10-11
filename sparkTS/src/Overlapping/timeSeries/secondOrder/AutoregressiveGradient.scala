package overlapping.timeSeries.secondOrder

import breeze.linalg.{DenseMatrix, DenseVector}
import overlapping._
import overlapping.timeSeries.{ModelSize, SecondOrderEssStat}

/**
 * Created by Francois Belletti on 9/24/15.
 */
class AutoregressiveGradient[IndexT <: Ordered[IndexT]](
  val p: Int,
  val deltaT: Double,
  val x : Array[DenseMatrix[Double]],
  val gradientFunction: (Array[DenseMatrix[Double]], Array[(IndexT, DenseVector[Double])]) => Array[DenseMatrix[Double]])
extends SecondOrderEssStat[IndexT, DenseVector[Double], Array[DenseMatrix[Double]]]
{

  val gradientSizes = x.map(y => (y.rows, y.cols))

  def kernelWidth = IntervalSize(p * deltaT, 0)

  def modelOrder = ModelSize(p, 0)

  def zero = gradientSizes.map({case (nRows, nCols) => DenseMatrix.zeros[Double](nRows, nCols)})

  def setNewX(newX: Array[DenseMatrix[Double]]) = {
    for(i <- x.indices){
      x(i) := newX(i)
    }
  }

  def getGradientSize = gradientSizes

  override def kernel(slice: Array[(IndexT, DenseVector[Double])]): Array[DenseMatrix[Double]] = {

    if(slice.length != modelWidth){
      return gradientSizes.map({case (r, c) => DenseMatrix.zeros[Double](r, c)})
    }
    gradientFunction(x, slice)

  }

  override def reducer(x: Array[DenseMatrix[Double]], y: Array[DenseMatrix[Double]]): Array[DenseMatrix[Double]] ={
    x.zip(y).map({case (x, y) => x + y})
  }

}

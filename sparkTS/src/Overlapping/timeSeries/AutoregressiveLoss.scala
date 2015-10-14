package overlapping.timeSeries

import breeze.linalg.{DenseMatrix, DenseVector}
import overlapping._

/**
 * Created by Francois Belletti on 9/24/15.
 */
class AutoregressiveLoss[IndexT <: Ordered[IndexT]](
  p: Int,
  lossFunction: (Array[DenseMatrix[Double]], Array[(IndexT, DenseVector[Double])]) => Double,
  dim: Option[Int] = None)
  (implicit config: TSConfig)
extends SecondOrderEssStat[IndexT, DenseVector[Double], Double]
{

  val d = dim.getOrElse(config.d)
  val x = Array.fill(p){DenseMatrix.zeros[Double](d, d)}

  def kernelWidth = IntervalSize(p * config.deltaT, 0)

  def modelOrder = ModelSize(p, 0)

  def zero = 0.0

  def setNewX(newX: Array[DenseMatrix[Double]]) = {
    for(i <- newX.indices){
      x(i) := newX(i)
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
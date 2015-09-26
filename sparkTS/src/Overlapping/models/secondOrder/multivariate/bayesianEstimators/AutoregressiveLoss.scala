package overlapping.models.secondOrder.multivariate.bayesianEstimators

import breeze.linalg.{DenseMatrix, DenseVector}
import overlapping.IntervalSize
import overlapping.models.secondOrder.{ModelSize, SecondOrderEssStat}

/**
 * Created by Francois Belletti on 9/24/15.
 */
class AutoregressiveLoss[IndexT <: Ordered[IndexT]](
  val p: Int,
  val deltaT: Double,
  val x : Array[DenseMatrix[Double]],
  val lossFunction: (Array[DenseMatrix[Double]], Array[(IndexT, DenseVector[Double])]) => Double
)
extends SecondOrderEssStat[IndexT, DenseVector[Double], Double]
{

  def kernelWidth = IntervalSize(p * deltaT, 0)

  def modelOrder = ModelSize(p, 0)

  def zero = 0.0

  def setNewX(newX: Array[DenseMatrix[Double]]) = {
    for(i <- x.indices){
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

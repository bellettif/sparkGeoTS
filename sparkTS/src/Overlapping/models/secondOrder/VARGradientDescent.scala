package overlapping.models.secondOrder

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import overlapping.IntervalSize
import overlapping.containers.block.SingleAxisBlock
import overlapping.models.secondOrder.procedures.{L1GradientDescent, GradientDescent}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/16/15.
 */
class VARGradientDescent[IndexT <: Ordered[IndexT] : ClassTag](
  val modelOrder: Int,
  val deltaT: Double,
  val lossFunction: (Array[DenseMatrix[Double]], Array[(IndexT, DenseVector[Double])]) => Double,
  val gradientFunction: (Array[DenseMatrix[Double]], Array[(IndexT, DenseVector[Double])]) => Array[DenseMatrix[Double]],
  val gradientSizes: Array[(Int, Int)],
  val stepSize: Int => Double,
  val precision: Double,
  val maxIter: Int,
  val start: Array[DenseMatrix[Double]])
  extends SecondOrderModel[IndexT, DenseVector[Double]]{

  lazy val gradientIndices = gradientSizes.indices.toArray

  def sumArrays(x: Array[DenseMatrix[Double]], y: Array[DenseMatrix[Double]]): Array[DenseMatrix[Double]] ={
    x.zip(y).map({case (x, y) => x + y})
  }

  def sumLosses(x: Double, y: Double): Double ={
    x + y
  }

  /*
  Kernel level computation
   */
  def gradientKernel(parameters: Array[DenseMatrix[Double]],
                     slice: Array[(IndexT, DenseVector[Double])]): Array[DenseMatrix[Double]] = {
    if(slice.length != modelOrder + 1){
      return gradientSizes.map({case (r, c) => DenseMatrix.zeros[Double](r, c)})
    }
    gradientFunction(parameters, slice)
  }

  def lossKernel(parameters: Array[DenseMatrix[Double]], slice: Array[(IndexT, DenseVector[Double])]): Double = {
    if(slice.length != modelOrder + 1){
      return 0.0
    }
    lossFunction(parameters, slice)
  }

  /*
  Slice level computation
   */
  def computeGradient(parameters: Array[DenseMatrix[Double]],
                      slice: Array[(IndexT, DenseVector[Double])]): Array[DenseMatrix[Double]] = {
    slice
      .sliding(modelOrder + 1)
      .map(gradientKernel(parameters, _))
      .reduce(sumArrays)
  }

  def computeLoss(parameters: Array[DenseMatrix[Double]],
                  slice: Array[(IndexT, DenseVector[Double])]): Double = {
    slice
      .sliding(modelOrder + 1)
      .map(lossKernel(parameters, _))
      .sum
  }

  def computeGradient(parameters: Array[DenseMatrix[Double]],
                      timeSeries: SingleAxisBlock[IndexT, DenseVector[Double]]): Array[DenseMatrix[Double]] = {
    val selectionSize = IntervalSize(modelOrder * deltaT, 0)
    timeSeries
      .slidingFold(Array(selectionSize))(
        gradientKernel(parameters, _),
        gradientSizes.map({case (r, c) => DenseMatrix.zeros[Double](r, c)}),
        sumArrays
      )
  }

  def computeLoss(parameters: Array[DenseMatrix[Double]],
                  timeSeries: SingleAxisBlock[IndexT, DenseVector[Double]]): Double = {
    val selectionSize = IntervalSize(modelOrder * deltaT, 0)
    timeSeries
      .slidingFold(Array(selectionSize))(
        lossKernel(parameters, _),
        0.0,
        sumLosses
      )
  }

  def computeGradient(parameters: Array[DenseMatrix[Double]],
                      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): Array[DenseMatrix[Double]] = {
    timeSeries
      .mapValues(computeGradient(parameters, _))
      .map(_._2)
      .reduce(sumArrays)
  }

  def computeLoss(parameters: Array[DenseMatrix[Double]],
                  timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): Double = {
    timeSeries
      .mapValues(computeLoss(parameters, _))
      .map(_._2)
      .reduce(_ + _)
  }

  override def estimate(slice: Array[(IndexT, DenseVector[Double])]): Array[DenseMatrix[Double]] = {
    GradientDescent.run[Array[(IndexT, DenseVector[Double])]](
      {case (param: Array[DenseMatrix[Double]], data: Array[(IndexT, DenseVector[Double])]) => computeLoss(param, data)},
      {case (param: Array[DenseMatrix[Double]], data: Array[(IndexT, DenseVector[Double])]) => computeGradient(param, data)},
      gradientSizes,
      stepSize,
      precision,
      maxIter,
      start,
      slice
    )

  }

  override def estimate(timeSeries: SingleAxisBlock[IndexT, DenseVector[Double]]): Array[DenseMatrix[Double]] = {
    GradientDescent.run[SingleAxisBlock[IndexT, DenseVector[Double]]](
      {case (param: Array[DenseMatrix[Double]], data: SingleAxisBlock[IndexT, DenseVector[Double]]) => computeLoss(param, data)},
      {case (param: Array[DenseMatrix[Double]], data: SingleAxisBlock[IndexT, DenseVector[Double]]) => computeGradient(param, data)},
      gradientSizes,
      stepSize,
      precision,
      maxIter,
      start,
      timeSeries
    )
  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): Array[DenseMatrix[Double]] = {

    timeSeries.persist(MEMORY_AND_DISK)

    val parameters = GradientDescent.run[RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]](
      {case (param: Array[DenseMatrix[Double]], data: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]) => computeLoss(param, data)},
      {case (param: Array[DenseMatrix[Double]], data: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]) => computeGradient(param, data)},
      gradientSizes,
      stepSize,
      precision,
      maxIter,
      start,
      timeSeries
    )

    timeSeries.unpersist(false)

    parameters

  }

}

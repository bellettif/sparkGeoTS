package main.scala.overlapping.timeSeries

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import main.scala.overlapping.containers._
import main.scala.overlapping.timeSeries.secondOrder.multivariate.frequentistEstimators.procedures.ToeplitzMulti

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
object VARModel{

  def apply[IndexT <: TSInstant[IndexT] : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      p: Int,
      mean: Option[DenseVector[Double]] = None): (Array[DenseMatrix[Double]], DenseMatrix[Double]) = {

    val estimator = new VARModel[IndexT](
      p,
      timeSeries.config,
      timeSeries.content.context.broadcast(mean))

    estimator.estimate(timeSeries)

  }

}

class VARModel[IndexT <: TSInstant[IndexT] : ClassTag](
    p: Int,
    config: VectTSConfig[IndexT],
    mean: Broadcast[Option[DenseVector[Double]]])
  extends CrossCovariance[IndexT](p, config, mean){

  def estimateVARMatrices(crossCovMatrices: Array[DenseMatrix[Double]], covMatrix: DenseMatrix[Double]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) ={
    val nCols = covMatrix.rows

    val coeffMatrices = ToeplitzMulti(p, nCols,
      crossCovMatrices.slice(1, 2 * p),
      crossCovMatrices.slice(p + 1, 2 * p + 1))

    coeffMatrices.foreach(x => x := x.t)

    var noiseVariance = covMatrix
    for(i <- 1 to p){
      noiseVariance :+= - coeffMatrices(i - 1) * crossCovMatrices(p - i)
    }

    (coeffMatrices, noiseVariance)
  }

  override def estimate(timeSeries: TimeSeries[IndexT, DenseVector[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double])= {

    val (crossCovMatrices, covMatrix) = super.estimate(timeSeries)
    estimateVARMatrices(crossCovMatrices, covMatrix)

  }

}
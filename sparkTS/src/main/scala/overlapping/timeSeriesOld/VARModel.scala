package main.scala.overlapping.timeSeriesOld

import breeze.linalg.{DenseMatrix, DenseVector}
import main.scala.overlapping.timeSeriesOld.CrossCovariance
import main.scala.overlapping.timeSeriesOld.secondOrder.multivariate.frequentistEstimators.procedures.ToeplitzMulti
import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/13/15.
 */
object VARModel{

  def apply[IndexT : ClassTag](
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

class VARModel[IndexT : ClassTag](
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

  override def estimate(timeSeries: VectTimeSeries[IndexT]): (Array[DenseMatrix[Double]], DenseMatrix[Double])= {

    val (crossCovMatrices, covMatrix) = super.estimate(timeSeries)
    estimateVARMatrices(crossCovMatrices, covMatrix)

  }

}
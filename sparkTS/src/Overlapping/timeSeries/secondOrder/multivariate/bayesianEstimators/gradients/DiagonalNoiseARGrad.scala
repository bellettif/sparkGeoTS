package overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.gradients

import breeze.linalg.{DenseVector, DenseMatrix}
import org.apache.spark.broadcast.Broadcast

import overlapping.timeSeries._

/**
 * Created by Francois Belletti on 9/28/15.
 */
class DiagonalNoiseARGrad[IndexT <: Ordered[IndexT]](
   val sigmaEps: DenseVector[Double],
   val nSamples: Long,
   val mean: Broadcast[DenseVector[Double]])
  extends Serializable{

  val d = sigmaEps.size
  val precisionMatrix = DenseVector.ones[Double](d)
  precisionMatrix :/= sigmaEps

  def apply(params: Array[DenseMatrix[Double]],
            data: Array[(IndexT, DenseVector[Double])]): Array[DenseMatrix[Double]] = {

    val p = params.length
    val totGradient   = Array.fill(p){DenseMatrix.zeros[Double](d, d)}
    val prevision     = DenseVector.zeros[Double](d)

    val meanValue = mean.value

    for(i <- p until data.length){
      prevision := 0.0
      for(h <- 1 to p){
        prevision += params(h - 1) * (data(i - h)._2 - meanValue)
      }
      for(h <- 1 to p){
        totGradient(h - 1) :-= (data(i)._2 - prevision) * (data(i - h)._2 - meanValue).t
      }
    }

    for(h <- 1 to p) {
      for(i <- 0 until d) {
        totGradient(h - 1)(i, ::) :*= precisionMatrix(i) * 2.0 / nSamples.toDouble
      }
    }

    totGradient
  }

}

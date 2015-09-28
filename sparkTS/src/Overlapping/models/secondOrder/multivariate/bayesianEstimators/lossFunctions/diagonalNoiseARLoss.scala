package overlapping.models.secondOrder.multivariate.bayesianEstimators.lossFunctions

import breeze.linalg.{sum, DenseVector, DenseMatrix}
import org.apache.spark.broadcast.Broadcast
import overlapping.surrogateData.TSInstant

/**
 * Created by Francois Belletti on 9/28/15.
 */
class DiagonalNoiseARLoss(
   val sigmaEps: DenseVector[Double],
   val nSamples: Long,
   val mean: Broadcast[DenseVector[Double]]
  )
  extends Serializable{

  val d = sigmaEps.size
  val precisionMatrix = DenseVector.ones[Double](d)
  precisionMatrix :/= sigmaEps

  def apply(params: Array[DenseMatrix[Double]],
            data: Array[(TSInstant, DenseVector[Double])]): Double = {

    val p = params.length
    var totError = 0.0
    val prevision = DenseVector.zeros[Double](d)
    val error     = DenseVector.zeros[Double](d)

    val meanValue = mean.value

    for(i <- p until data.length){

      prevision := 0.0

      for(h <- 1 to p){
        prevision += params(h - 1) * (data(i - h)._2 - meanValue)
      }

      error := data(i)._2 - meanValue - prevision

      for(j <- 0 until d){
        totError += error(j) * error(j) * precisionMatrix(j)
      }
    }

    totError / nSamples.toDouble
  }

}

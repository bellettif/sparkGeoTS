package overlapping.models.secondOrder.procedures

import breeze.linalg._
import breeze.numerics.{abs, sqrt}

/**
 * Created by Francois Belletti on 7/14/15.
 */

object L1TruncatedGradientDescent extends Serializable{

  def run[DataT](lossFunction: (Array[DenseMatrix[Double]], DataT) => Double,
                 gradientFunction: (Array[DenseMatrix[Double]], DataT) => Array[DenseMatrix[Double]],
                 gradientSizes: Array[(Int, Int)],
                 stepSize: Int => Double,
                 precision: Double,
                 theta: Double,
                 lambda: Double,
                 maxIter: Int,
                 start: Array[DenseMatrix[Double]],
                 data: DataT): Array[DenseMatrix[Double]] ={

    var prevLoss = lossFunction(start, data)
    var nextLoss = prevLoss

    var firstIter = true

    var parameters = start
    var gradient = Array.fill(gradientSizes.length) {
      DenseMatrix.zeros[Double](0, 0)
    }

    var gradientMagnitude = 0.0

    println("Initial loss")
    println(prevLoss)

    var i = 0

    while (firstIter || ((i <= maxIter) && abs(prevLoss - nextLoss) > precision)) {

      println(i)
      println("Parameters")
      parameters.foreach(x => {println(x); println()})

      gradient = gradientFunction(parameters, data)

      println("Gradient")
      gradient.foreach(x => {println(x); println()})

      parameters = parameters.indices.toArray.map(x => parameters(x) - (gradient(x) * stepSize(i)))

      for ((gradientSize, j) <- gradientSizes.zipWithIndex) {
        for (k <- 0 until gradientSize._1) {
          for (l <- 0 until gradientSize._2) {
            val paramValue = parameters(j)(k, l)
            if ((paramValue < 0) && (-theta <= paramValue)) {
              parameters(j)(k, l) = min(0.0, paramValue + stepSize(i) * lambda)
              gradient(j)(k, l) = (parameters(j)(k, l) - paramValue) / stepSize(i)
            }
            if ((paramValue > 0) && (paramValue <= theta)) {
              parameters(j)(k, l) = max(0.0, paramValue - stepSize(i) * lambda)
              gradient(j)(k, l) = (parameters(j)(k, l) - paramValue) / stepSize(i)
            }
          }
        }
      }

      gradientMagnitude = sqrt(gradient.map({case x: DenseMatrix[Double] => sum(x :* x)}).sum)

      i = i + 1
      prevLoss = nextLoss
      nextLoss = lossFunction(parameters, data) + gradient.map({case x: DenseMatrix[Double] => sum(abs(x))}).sum

      println("New parameters")
      parameters.foreach(x => {println(x); println()})

      println("Loss")
      println(nextLoss)
      println("-----------------------------")

      firstIter = false
    }

    parameters
  }

}

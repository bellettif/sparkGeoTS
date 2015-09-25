package overlapping.models.secondOrder.multivariate.bayesianEstimators.procedures

import breeze.linalg._
import breeze.numerics.sqrt

/**
 * Created by Francois Belletti on 7/14/15.
 */

object GradientDescent extends Serializable{

  def run[DataT](lossFunction: (Array[DenseMatrix[Double]], DataT) => Double,
                 gradientFunction: (Array[DenseMatrix[Double]], DataT) => Array[DenseMatrix[Double]],
                 gradientSizes: Array[(Int, Int)],
                 stepSize: Int => Double,
                 precision: Double,
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

    while (firstIter || ((i <= maxIter) && (gradientMagnitude > precision * (1 + nextLoss)))) {

      println(i)
      println("Parameters")
      parameters.foreach(x => {println(x); println()})

      gradient = gradientFunction(parameters, data)

      println("Gradient")
      gradient.foreach(x => {println(x); println()})

      gradientMagnitude = sqrt(gradient.map({case x: DenseMatrix[Double] => sum(x :* x)}).sum)

      parameters = parameters.indices.toArray.map(x => parameters(x) - (gradient(x) * stepSize(i)))
      i = i + 1
      prevLoss = nextLoss
      nextLoss = lossFunction(parameters, data)

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

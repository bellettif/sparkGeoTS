package overlapping.models.secondOrder.procedures

import breeze.linalg._

/**
 * Created by Francois Belletti on 7/14/15.
 */

object Rybicki extends Serializable{

  /*
  This procedures inverts a non-symmetric Toeplitz matrix.
  R is a (n * n) Toeplitz matrix only characterized by its 2*n - 1 distinct elements.
  The R matrix is described from its upper left corner to its lower right corner.
  Check out Numerical Recipes Third Edition p97.
  TODO: check size of toepM and y are compatible.
  TODO: check that diagonal element of toepM is non zero.
   */
  def apply(n: Int, R: DenseVector[Double], y: DenseVector[Double]): DenseVector[Double] ={
    var prevX = DenseVector.zeros[Double](1)
    var prevH = DenseVector.zeros[Double](1)
    var prevG = DenseVector.zeros[Double](1)

    /*
    Equation system 2.8.26
     */
    prevX(0) = y(0) / R(n - 1)       // R(n - 1) corresponds to R(0) in the book
    prevG(0) = R(n - 2) / R(n - 1)
    prevH(0) = R(n) / R(n - 1)

    var nextH = DenseVector.zeros[Double](2)
    var nextG = DenseVector.zeros[Double](2)
    var nextX = DenseVector.zeros[Double](2)
    for(m <- 0 until n - 1){
      println(prevG)
      println(prevH)
      println(prevX)
      println()

      if(m < n - 2) {
        // Equation 2.8.23
        nextH(m + 1) = (sum(R(m + n to n by -1) :* prevH) - R(m + n + 1)) / (sum(R(m + n to n by -1) :* reverse(prevG)) - R(n - 1))

        // Equation 2.8.24
        nextG(m + 1) = (sum(R(n - m - 2 to n - 2) :* prevG) - R(n - m - 3)) / (sum(R(n - m - 2 to n - 2) :* reverse(prevH)) - R(n - 1))

        // Equation system 2.8.25
        nextH(0 to m) := prevH - (reverse(prevG) * nextH(m + 1))
        nextG(0 to m) := prevG - (reverse(prevH) * nextG(m + 1))
      }

      // Equation 2.8.19
      // TODO: computation of numerator is redundent with that of nextH
      nextX(m + 1) = (sum(R(m + n to n by -1) :* prevX) - y(m + 1)) / (sum(R(m + n to n by - 1) :* reverse(prevG)) - R(n - 1))

      // Equation 2.8.16
      nextX(0 to m) := prevX - (reverse(prevG) * nextX(m + 1))

      println(nextG)
      println(nextH)
      println(nextX)
      println()

      //Swap and allocate
      prevX = nextX
      prevH = nextH
      prevG = nextG

      nextX = DenseVector.zeros[Double](m + 3)
      nextG = DenseVector.zeros[Double](m + 3)
      nextH = DenseVector.zeros[Double](m + 3)

    }

    prevX
  }

}

package overlapping.containers.tests

/**
 * Created by Francois Belletti on 8/17/15.
 */

import breeze.linalg.{DenseMatrix, DenseVector}
import org.scalatest.{FlatSpec, Matchers}

import overlapping.models.secondOrder.procedures.Rybicki

/**
 * Created by Francois Belletti on 8/4/15.
 */
class TestRybicki extends FlatSpec with Matchers{

  "The Ribicky procedure " should " properly solve a full Toeplitz system" in {

    val n       = 3
    val RVector = DenseVector.rand[Double](2 * n - 1)
    val y       = DenseVector.rand[Double](n)

    val toeplitzMatrix = DenseMatrix.zeros[Double](n, n)

    for(i <- 0 until n){
      for(j <- 0 until n){
        toeplitzMatrix(i, j) = RVector(i + n - j - 1)
      }
    }

    val x = Rybicki(n, RVector, y)

    val yCheck: DenseVector[Double] = toeplitzMatrix * x

    for(i <- 0 until n){
      y(i) should be (yCheck(i) +- 0.0000001)
    }

  }

}
package TsUtils

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import TsUtils.Procedures.Rybicki

object BreezeBoilerPlate {

  def main(args: Array[String]): Unit ={

    /*
    This was designed to test the Rybicki procedure for non symmetric Toeplitz matrix inversion
     */

    val n = 4
    val R = DenseVector.rand[Double](2 * n - 1)
    val x = DenseVector.rand[Double](n)

    val RMatrix = DenseMatrix.zeros[Double](n, n)
    for(i <- 0 until n){
      for(j <- 0 until n){
        RMatrix(i, j) = R(i + (n - 1 - j))
      }
    }

    val y = RMatrix * x

    /*
    val x_sol = Rybicki.runR(n, R, y)

    println(x)
    println(x_sol)

    */


  }

}

package main.scala.overlapping.analytics

import breeze.linalg._
import breeze.stats.distributions.Rand
import main.scala.overlapping.containers.TSInstant
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Simulate data based on different models.
 */
object Surrogate {


  def generateWhiteNoise[IndexT : TSInstant](
      nColumns: Int,
      nSamples: Int,
      deltaT: IndexT,
      noiseGen: Rand[Double],
      magnitudes: DenseVector[Double],
      sc: SparkContext): RDD[(IndexT, DenseVector[Double])] = {

    val rawData = (0 until nSamples)
      .map(x => (implicitly[TSInstant[IndexT]].times(deltaT, x),
                 magnitudes :* DenseVector(noiseGen.sample(nColumns).toArray)))

    sc.parallelize(rawData)

  }

  def generateOnes[IndexT : TSInstant](
      nColumns: Int,
      nSamples: Int,
      deltaT: IndexT,
      sc: SparkContext): RDD[(IndexT, DenseVector[Double])] = {

    val rawData = (0 until nSamples)
      .map(x => (implicitly[TSInstant[IndexT]].times(deltaT, x),
                 DenseVector.ones[Double](nColumns)))

    sc.parallelize(rawData)

  }

  def generateAR[IndexT : TSInstant](
      phis: Array[DenseVector[Double]],
      nColumns:Int,
      nSamples: Int,
      deltaT: IndexT,
      noiseGen: Rand[Double],
      magnitudes: DenseVector[Double],
      sc: SparkContext): RDD[(IndexT, DenseVector[Double])] = {

    val d = phis.length
    val p = phis(0).length

    val phiMatrices = Array.fill(p)(DenseMatrix.zeros[Double](d, d))

    for(i <- 0 until p){
      for(j <- 0 until d){
        phiMatrices(i)(j, j) = phis(j)(i)
      }
    }

    generateVAR(phiMatrices, nColumns, nSamples, deltaT, noiseGen, magnitudes, sc)

  }

  /**
   * Generate independent Moving Average(q) time series.
   * The parameters are organized as an array of vector
   * (one vector per dimension, all must be of length q).
   *
   * @param thetas
   * @param nColumns
   * @param nSamples
   * @param deltaT
   * @param noiseGen
   * @param magnitudes
   * @param sc
   * @tparam IndexT
   * @return
   */
  def generateMA[IndexT : TSInstant](
      thetas: Array[DenseVector[Double]],
      nColumns: Int,
      nSamples: Int,
      deltaT: IndexT,
      noiseGen: Rand[Double],
      magnitudes: DenseVector[Double],
      sc: SparkContext): RDD[(IndexT, DenseVector[Double])] = {

    val d = thetas.length
    val p = thetas(0).length

    val thetaMatrices = Array.fill(p)(DenseMatrix.zeros[Double](d, d))

    for(i <- 0 until p){
      for(j <- 0 until d){
        thetaMatrices(i)(j, j) = thetas(j)(i)
      }
    }

    generateVMA(thetaMatrices, nColumns, nSamples, deltaT, noiseGen, magnitudes, sc)

  }

  def generateARMA[IndexT : TSInstant](
      phis: Array[DenseVector[Double]],
      thetas: Array[DenseVector[Double]],
      nColumns: Int,
      nSamples: Int,
      deltaT: IndexT,
      noiseGen: Rand[Double],
      magnitudes: DenseVector[Double],
      sc: SparkContext): RDD[(IndexT, DenseVector[Double])] = {


    val d = thetas.length
    val p = thetas(0).length

    val phiMatrices = Array.fill(p)(DenseMatrix.zeros[Double](d, d))

    for(i <- 0 until p){
      for(j <- 0 until d){
        phiMatrices(i)(j, j) = phis(j)(i)
      }
    }

    val thetaMatrices = Array.fill(p)(DenseMatrix.zeros[Double](d, d))

    for(i <- 0 until p){
      for(j <- 0 until d){
        thetaMatrices(i)(j, j) = thetas(j)(i)
      }
    }

    generateVARMA(phiMatrices, thetaMatrices, nColumns, nSamples, deltaT, noiseGen, magnitudes, sc)

  }

  def generateVAR[IndexT : TSInstant](
      phis: Array[DenseMatrix[Double]],
      nColumns:Int,
      nSamples: Int,
      deltaT: IndexT,
      noiseGen: Rand[Double],
      magnitudes: DenseVector[Double],
      sc: SparkContext): RDD[(IndexT, DenseVector[Double])] = {

    val p = phis.length

    val noiseMatrix = new DenseMatrix(nColumns, nSamples, noiseGen.sample(nSamples * nColumns).toArray)

    for(i <- p until nSamples){
      for(h <- 1 to p){
        noiseMatrix(::, i) += phis(h - 1) * (magnitudes :* noiseMatrix(::, i - h))
      }
    }

    val rawData = (0 until nSamples)
      .map(x => (implicitly[TSInstant[IndexT]].times(deltaT, x), noiseMatrix(::, x).copy))

    sc.parallelize(rawData)

  }

  def generateVMA[IndexT : TSInstant](
      thetas: Array[DenseMatrix[Double]],
      nColumns: Int,
      nSamples: Int,
      deltaT: IndexT,
      noiseGen: Rand[Double],
      magnitudes: DenseVector[Double],
      sc: SparkContext): RDD[(IndexT, DenseVector[Double])] = {

    val q = thetas.length

    val noiseMatrix = new DenseMatrix(nColumns, nSamples, noiseGen.sample(nSamples * nColumns).toArray)
    for(i <- (nSamples - 1) to 1 by -1){
      for(h <- 1 to q) {
        noiseMatrix(::, i) :+= thetas(h - 1) * (magnitudes :* noiseMatrix(::, i - h))
      }
    }

    val rawData = (0 until nSamples)
      .map(x => (implicitly[TSInstant[IndexT]].times(deltaT, x), noiseMatrix(::, x).copy))

    sc.parallelize(rawData)

  }

  def generateVARMA[IndexT : TSInstant](
      phis: Array[DenseMatrix[Double]],
      thetas: Array[DenseMatrix[Double]],
      nColumns: Int,
      nSamples: Int,
      deltaT: IndexT,
      noiseGen: Rand[Double],
      magnitudes: DenseVector[Double],
      sc: SparkContext): RDD[(IndexT, DenseVector[Double])] = {

    val q = thetas.length
    val p = phis.length

    val noiseMatrix = new DenseMatrix(nColumns, nSamples, noiseGen.sample(nSamples * nColumns).toArray)
    for(i <- (nSamples - 1) to q by -1){
      for(h <- 1 to q) {
        noiseMatrix(::, i) :+= thetas(h - 1) * (magnitudes :* noiseMatrix(::, i - h))
      }
    }

    for(i <- p until nSamples){
      for(h <- 1 to p){
        noiseMatrix(::, i) :+= phis(h - 1) * noiseMatrix(::, i - h)
      }
    }

    val rawData = (0 until nSamples)
      .map(x => (implicitly[TSInstant[IndexT]].times(deltaT, x), noiseMatrix(::, x).copy))

    sc.parallelize(rawData)

  }

}

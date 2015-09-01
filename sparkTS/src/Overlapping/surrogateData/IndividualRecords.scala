package overlapping.surrogateData

import breeze.linalg._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
 * Simulate data based on different models.
 */
object IndividualRecords {


  def generateWhiteNoise(nColumns: Int, nSamples: Int, deltaTMillis: Long,
                         sc: SparkContext): RDD[(TSInstant, Array[Double])] = {
    val meanValue = DenseVector.ones[Double](nColumns) * 0.5
    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)), (DenseVector.rand[Double](nColumns) - meanValue).toArray))
    sc.parallelize(rawData).asInstanceOf[RDD[(TSInstant, Array[Double])]]
  }

  def generateOnes(nColumns: Int, nSamples: Int, deltaTMillis: Long,
                   sc: SparkContext): RDD[(TSInstant, Array[Double])] = {
    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)), DenseVector.ones[Double](nColumns).toArray))
    sc.parallelize(rawData)
  }

  def generateAR1(phi1: Double, nColumns: Int, nSamples: Int, deltaTMillis: Long,
                  sc: SparkContext): RDD[(TSInstant, Array[Double])] = {
    val noiseMatrix   = DenseMatrix.rand[Double](nSamples, nColumns) - 0.5
    for(i <- 1 until nSamples){
      noiseMatrix(i, ::) :+= noiseMatrix(i-1, ::) :* phi1
    }

    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)), noiseMatrix(x, ::).t.toArray))

    sc.parallelize(rawData)
  }

  def generateAR2(phi1: Double, phi2: Double, nColumns: Int, nSamples: Int, deltaTMillis: Long,
                  sc: SparkContext): RDD[(TSInstant, Array[Double])] = {
    val noiseMatrix   = DenseMatrix.rand[Double](nSamples, nColumns) - 0.5
    for(i <- 2 until nSamples){
      noiseMatrix(i, ::) :+= (noiseMatrix(i-1, ::) :* phi1) :+ (noiseMatrix(i-2, ::) :* phi2)
    }

    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)), noiseMatrix(x, ::).t.toArray))

    sc.parallelize(rawData)
  }

  def generateAR(phis: Array[Double], nColumns:Int, nSamples: Int, deltaTMillis: Long,
                 sc: SparkContext): RDD[(TSInstant, Array[Double])] = {

    val p = phis.length

    val noiseMatrix   = DenseMatrix.rand[Double](nSamples, nColumns) - 0.5
    for(i <- p until nSamples){
      for(h <- 1 to p){
        noiseMatrix(i, ::) :+= (noiseMatrix(i - h, ::) :* phis(h - 1))
      }
    }

    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)), noiseMatrix(x, ::).t.toArray))

    sc.parallelize(rawData)
  }

  def generateMA1(theta1: Double, nColumns: Int, nSamples: Int, deltaTMillis: Long,
                  sc: SparkContext): RDD[(TSInstant, Array[Double])] = {
    val noiseMatrix   = DenseMatrix.rand[Double](nSamples, nColumns) - 0.5
    for(i <- (nSamples - 1) to 1 by -1){
      noiseMatrix(i, ::) :+= noiseMatrix(i-1, ::) :* theta1
    }

    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)), noiseMatrix(x, ::).t.toArray))

    sc.parallelize(rawData)
  }

  def generateMA(thetas: Array[Double], nColumns: Int, nSamples: Int, deltaTMillis: Long,
                 sc: SparkContext): RDD[(TSInstant, Array[Double])] = {
    val q = thetas.length

    val noiseMatrix   = DenseMatrix.rand[Double](nSamples, nColumns) - 0.5
    for(i <- (nSamples - 1) to 1 by -1){
      for(h <- 1 to q) {
        noiseMatrix(i, ::) :+= noiseMatrix(i - h, ::) :* thetas(h - 1)
      }
    }

    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)), noiseMatrix(x, ::).t.toArray))

    sc.parallelize(rawData)

  }

  def generateARMA(phis: Array[Double], thetas: Array[Double],
                   nColumns: Int, nSamples: Int, deltaTMillis: Long,
                   sc: SparkContext): RDD[(TSInstant, Array[Double])] = {

    val q = thetas.length
    val p = phis.length

    val noiseMatrix   = DenseMatrix.rand[Double](nSamples, nColumns) - 0.5
    for(i <- (nSamples - 1) to 1 by -1){
      for(h <- 1 to q) {
        noiseMatrix(i, ::) :+= noiseMatrix(i - h, ::) :* thetas(h - 1)
      }
    }

    for(i <- p until nSamples){
      for(h <- 1 to p){
        noiseMatrix(i, ::) :+= (noiseMatrix(i - h, ::) :* phis(h - 1))
      }
    }

    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)), noiseMatrix(x, ::).t.toArray))

    sc.parallelize(rawData)

  }

}

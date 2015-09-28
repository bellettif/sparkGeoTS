package overlapping.surrogateData

import breeze.linalg._
import breeze.stats.distributions
import breeze.stats.distributions.Rand
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
 * Simulate data based on different models.
 */
object IndividualRecords {


  def generateWhiteNoise(nColumns: Int,
                         nSamples: Int,
                         deltaTMillis: Long,
                         noiseGen: Rand[Double],
                         magnitudes: DenseVector[Double],
                         sc: SparkContext):
  RDD[(TSInstant, DenseVector[Double])] = {
    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)),
                 magnitudes :* DenseVector(noiseGen.sample(nColumns).toArray)))
    sc.parallelize(rawData)
  }

  def generateOnes(nColumns: Int,
                   nSamples: Int,
                   deltaTMillis: Long,
                   sc: SparkContext):
  RDD[(TSInstant, DenseVector[Double])] = {
    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)),
                 DenseVector.ones[Double](nColumns)))
    sc.parallelize(rawData)
  }

  def generateAR(phis: Array[Double],
                 nColumns:Int,
                 nSamples: Int,
                 deltaTMillis: Long,
                 noiseGen: Rand[Double],
                 magnitudes: DenseVector[Double],
                 sc: SparkContext): RDD[(TSInstant, DenseVector[Double])] = {

    val p = phis.length

    val noiseMatrix = new DenseMatrix(nSamples, nColumns, noiseGen.sample(nSamples * nColumns).toArray)
    for(i <- p until nSamples){
      for(h <- 1 to p){
        noiseMatrix(i, ::) :+= (magnitudes.t :* noiseMatrix(i - h, ::)) :* phis(h - 1)
      }
    }

    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)), noiseMatrix(x, ::).t.copy))

    sc.parallelize(rawData)
  }

  def generateMA(thetas: Array[Double],
                 nColumns: Int,
                 nSamples: Int,
                 deltaTMillis: Long,
                 noiseGen: Rand[Double],
                 magnitudes: DenseVector[Double],
                 sc: SparkContext): RDD[(TSInstant, DenseVector[Double])] = {
    val q = thetas.length

    val noiseMatrix = new DenseMatrix(nSamples, nColumns, noiseGen.sample(nSamples * nColumns).toArray)
    for(i <- (nSamples - 1) to q by -1){
      for(h <- 1 to q) {
        noiseMatrix(i, ::) :+= (magnitudes.t :* noiseMatrix(i - h, ::)) :* thetas(h - 1)
      }
    }

    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)), noiseMatrix(x, ::).t.copy))

    sc.parallelize(rawData)

  }

  def generateARMA(phis: Array[Double],
                   thetas: Array[Double],
                   nColumns: Int,
                   nSamples: Int,
                   deltaTMillis: Long,
                   noiseGen: Rand[Double],
                   magnitudes: DenseVector[Double],
                   sc: SparkContext): RDD[(TSInstant, Array[Double])] = {

    val q = thetas.length
    val p = phis.length

    val noiseMatrix = new DenseMatrix(nSamples, nColumns, noiseGen.sample(nSamples * nColumns).toArray)
    for(i <- (nSamples - 1) to q by -1){
      for(h <- 1 to q) {
        noiseMatrix(i, ::) :+= (magnitudes.t :* noiseMatrix(i - h, ::)) :* thetas(h - 1)
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

  def generateVAR(phis: Array[DenseMatrix[Double]],
                  nColumns:Int,
                  nSamples: Int,
                  deltaTMillis: Long,
                  noiseGen: Rand[Double],
                  magnitudes: DenseVector[Double],
                  sc: SparkContext):
  RDD[(TSInstant, DenseVector[Double])] = {

    val p = phis.length

    val noiseMatrix = new DenseMatrix(nColumns, nSamples, noiseGen.sample(nSamples * nColumns).toArray)

    for(i <- p until nSamples){
      for(h <- 1 to p){
        noiseMatrix(::, i) += phis(h - 1) * (magnitudes :* noiseMatrix(::, i - h))
      }
    }

    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)), noiseMatrix(::, x).copy))

    sc.parallelize(rawData)
  }

  def generateVMA(thetas: Array[DenseMatrix[Double]],
                  nColumns: Int,
                  nSamples: Int,
                  deltaTMillis: Long,
                  noiseGen: Rand[Double],
                  magnitudes: DenseVector[Double],
                  sc: SparkContext):
  RDD[(TSInstant, DenseVector[Double])] = {

    val q = thetas.length

    val noiseMatrix = new DenseMatrix(nColumns, nSamples, noiseGen.sample(nSamples * nColumns).toArray)
    for(i <- (nSamples - 1) to 1 by -1){
      for(h <- 1 to q) {
        noiseMatrix(::, i) :+= thetas(h - 1) * (magnitudes :* noiseMatrix(::, i - h))
      }
    }

    val rawData = (0 until nSamples)
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)), noiseMatrix(::, x).copy))

    sc.parallelize(rawData)

  }

  def generateVARMA(phis: Array[DenseMatrix[Double]],
                    thetas: Array[DenseMatrix[Double]],
                    nColumns: Int,
                    nSamples: Int,
                    deltaTMillis: Long,
                    noiseGen: Rand[Double],
                    magnitudes: DenseVector[Double],
                    sc: SparkContext): RDD[(TSInstant, DenseVector[Double])] = {

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
      .map(x => (TSInstant(new DateTime(x * deltaTMillis)), noiseMatrix(::, x).copy))

    sc.parallelize(rawData)

  }

}

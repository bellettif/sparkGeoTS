package overlapping.surrogateData

import breeze.linalg._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
 * Created by Francois Belletti on 8/7/15.
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

  /*
  def getAR2TsRDD(phi1: Double, phi2: Double, nColumns: Int, nSamples: Int, deltaTMillis: Long,
                  sc: SparkContext) = {
    val noiseMatrix   = DenseMatrix.rand[Double](nSamples, nColumns) - 0.5
    for(i <- 2 until nSamples){
      noiseMatrix(i, ::) :+= (noiseMatrix(i-1, ::) :* phi1) :+ (noiseMatrix(i-2, ::) :* phi2)
    }
    val rawData = (0 until nSamples)
      .map(x => x +: noiseMatrix(x, ::).t.toArray)
      .map(x => new DateTime(x.apply(0).asInstanceOf[Int].toLong * deltaTMillis) +: x.drop(1))
    sc.parallelize(rawData)
  }

  def getMA1TsRDD(theta1: Double, nColumns: Int, nSamples: Int, deltaTMillis: Long,
                  sc: SparkContext) = {
    val noiseMatrix   = DenseMatrix.rand[Double](nSamples, nColumns) - 0.5
    for(i <- (nSamples - 1) to 1 by -1){
      noiseMatrix(i, ::) :+= noiseMatrix(i-1, ::) :* theta1
    }
    val rawData = (0 until nSamples)
      .map(x => x +: noiseMatrix(x, ::).t.toArray)
      .map(x => new DateTime(x.apply(0).asInstanceOf[Int].toLong * deltaTMillis) +: x.drop(1))
    sc.parallelize(rawData)
  }

  def getOnesRawTsRDD(nColumns: Int, nSamples: Int, deltaTMillis: Long,
                      sc: SparkContext) = {
    val oneValue = DenseVector.ones[Double](nColumns)
    val rawData = (0 until nSamples)
      .map(x => x +: oneValue.toArray)
      .map(x => new DateTime(x.apply(0).asInstanceOf[Int].toLong * deltaTMillis) +: x.drop(1))
    sc.parallelize(rawData)
  }

  def getCumOnesRawTsRDD(nColumns: Int, nSamples: Int, deltaTMillis: Long,
                         sc: SparkContext) = {
    val oneValue = DenseVector.ones[Double](nColumns)
    val rawData = (0 until nSamples)
      .map(x => x +: (oneValue :* x.toDouble).toArray)
      .map(x => new DateTime(x.apply(0).asInstanceOf[Int].toLong * deltaTMillis) +: x.drop(1))
    sc.parallelize(rawData)
  }
  */

}

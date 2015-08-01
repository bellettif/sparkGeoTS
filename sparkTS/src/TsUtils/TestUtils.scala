package TsUtils

import org.joda.time.DateTime

import breeze.linalg._
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DoubleType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{TsDataFrame, Row, SQLContext}

/**
 * Created by Francois Belletti on 6/24/15.
 */
object TestUtils {


  def getJodaRandomTsRDD(nColumns: Int, nSamples: Int, deltaTMillis: Long,
                       sc: SparkContext) = {
    val meanValue = DenseVector.ones[Double](nColumns) * 0.5
    val rawData = (0 until nSamples)
      .map(x => x +: (DenseVector.rand[Double](nColumns) - meanValue).toArray)
      .map(x => new DateTime(x.apply(0).asInstanceOf[Int].toLong * deltaTMillis) +: x.drop(1))
    sc.parallelize(rawData)
  }


  def getRandomRawTsRDD(nColumns: Int, nSamples: Int, deltaTMillis: Long,
                        sc: SparkContext) = {
    val meanValue = DenseVector.ones[Double](nColumns) * 0.5
    val rawData = (0 until nSamples)
      .map(x => x +: (DenseVector.rand[Double](nColumns) - meanValue).toArray)
      .map(x => new DateTime(x.apply(0).asInstanceOf[Int].toLong * deltaTMillis) +: x.drop(1))
    sc.parallelize(rawData)
  }

  def getAR1TsRDD(phi1: Double, nColumns: Int, nSamples: Int, deltaTMillis: Long,
                  sc: SparkContext) = {
    val noiseMatrix   = DenseMatrix.rand[Double](nSamples, nColumns) - 0.5
    for(i <- 1 until nSamples){
      noiseMatrix(i, ::) :+= noiseMatrix(i-1, ::) :* phi1
    }
    val rawData = (0 until nSamples)
      .map(x => x +: noiseMatrix(x, ::).t.toArray)
      .map(x => new DateTime(x.apply(0).asInstanceOf[Int].toLong * deltaTMillis) +: x.drop(1))
    sc.parallelize(rawData)
  }

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

}

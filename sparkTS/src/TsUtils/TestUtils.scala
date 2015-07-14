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

  def getRandomTsDataFrame(nColumns: Int, nSamples: Int,
                           sc: SparkContext, sQLContext: SQLContext) = {
    val rawData = (0 until nSamples)
      .map(x => x +: DenseVector.rand[Double](nColumns).toArray)
      .map(x => new DateTime(x.apply(0).asInstanceOf[Int].toLong) +: x.drop(1))
    val rawDataRDD = sc.parallelize(rawData)

    val headers: Seq[String] = "Time" +: (1 until nColumns).map(x => "Link_" + x.toString)
    val schema = StructType(StructField(headers.head, TimestampType, false) +: headers.drop(1).map(x => StructField(x, DoubleType, false)))

    val rawDataFrame = sQLContext.createDataFrame(rawDataRDD.map(Row.fromSeq(_)), schema)
    new TsDataFrame(rawDataFrame, "Time")
  }

  def getJodaRandomTsRDD(nColumns: Int, nSamples: Int,
                       sc: SparkContext) = {
    val meanValue = DenseVector.ones[Double](nColumns) * 0.5
    val rawData = (0 until nSamples)
      .map(x => x +: (DenseVector.rand[Double](nColumns) - meanValue).toArray)
      .map(x => new DateTime(x.apply(0).asInstanceOf[Int].toLong * 50) +: x.drop(1))
    sc.parallelize(rawData)
  }


  def getRandomRawTsRDD(nColumns: Int, nSamples: Int,
                        sc: SparkContext) = {
    val meanValue = DenseVector.ones[Double](nColumns) * 0.5
    val rawData = (0 until nSamples)
      .map(x => x +: (DenseVector.rand[Double](nColumns) - meanValue).toArray)
      .map(x => new DateTime(x.apply(0).asInstanceOf[Int].toLong * 50) +: x.drop(1))
    sc.parallelize(rawData)
  }

  def getAR1TsRDD(rho1: Double, nColumns: Int, nSamples: Int,
                  sc: SparkContext) = {
    val noiseMatrix   = DenseMatrix.rand[Double](nSamples, nColumns) - 0.5
    for(i <- 1 until nSamples){
      noiseMatrix(i, ::) :+= noiseMatrix(i-1, ::) :* rho1
    }
    val rawData = (0 until nSamples)
      .map(x => x +: noiseMatrix(x, ::).t.toArray)
      .map(x => new DateTime(x.apply(0).asInstanceOf[Int].toLong * 50) +: x.drop(1))
    sc.parallelize(rawData)
  }

  def getAR2TsRDD(rho1: Double, rho2: Double, nColumns: Int, nSamples: Int,
                  sc: SparkContext) = {
    val noiseMatrix   = DenseMatrix.rand[Double](nSamples, nColumns) - 0.5
    for(i <- 2 until nSamples){
      noiseMatrix(i, ::) :+= (noiseMatrix(i-1, ::) :* rho1) :+ (noiseMatrix(i-2, ::) :* rho2)
    }
    val rawData = (0 until nSamples)
      .map(x => x +: noiseMatrix(x, ::).t.toArray)
      .map(x => new DateTime(x.apply(0).asInstanceOf[Int].toLong * 50) +: x.drop(1))
    sc.parallelize(rawData)
  }

  def getOnesRawTsRDD(nColumns: Int, nSamples: Int,
                      sc: SparkContext) = {
    val oneValue = DenseVector.ones[Double](nColumns)
    val rawData = (0 until nSamples)
      .map(x => x +: oneValue.toArray)
      .map(x => new DateTime(x.apply(0).asInstanceOf[Int].toLong * 50) +: x.drop(1))
    sc.parallelize(rawData)
  }

}

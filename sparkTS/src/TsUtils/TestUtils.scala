package TsUtils

import org.joda.time.DateTime

import breeze.linalg.DenseVector
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

  def getRandomRawTsRDD(nColumns: Int, nSamples: Int,
                        sc: SparkContext) = {
    val rawData = (0 until nSamples)
      .map(x => x +: DenseVector.rand[Double](nColumns).toArray)
      .map(x => new DateTime(x.apply(0).asInstanceOf[Int].toLong) +: x.drop(1))
    sc.parallelize(rawData)
  }

}

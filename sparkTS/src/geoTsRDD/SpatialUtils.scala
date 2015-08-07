package geoTsRDD

import breeze.linalg.DenseMatrix
import com.github.nscala_time.time.Imports._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.ScalaReflection.Schema

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
 * Created by Francois Belletti on 6/22/15.
 */
object SpatialUtils {

  def convertRowToEvent(row: Row, xIdx: Int, yIdx: Int, tIdx: Int,
                        attrIdx: Int, parseFormat: String):
  LocEvent[(Double, Double, DateTime), Double] ={
    new LocEvent[(Double, Double, DateTime), Double](
      (row.getAs[String](xIdx).toDouble,
        row.getAs[String](yIdx).toDouble,
        new DateTime(DateTimeFormat
          .forPattern(parseFormat)
          .parseDateTime(row.getAs[String](tIdx)))),
      row.getAs[String](attrIdx).toDouble)
  }

  /*
  * Convert space time rasters to a time index Spark dataFrame
   */
  def toTimeIndexedDf[AttribType](gridResX: Int, gridResY: Int,
                                  NAValue: AttribType,
                                  rasters: RDD[((Long, Long, Long), AttribType)],
                                  sqlContext: SQLContext): DataFrame = {

    val timeIdxRDD = rasters
      .map({ case ((x, y, t), v) => (t, ((x, y), v)) })
      .groupByKey()
      .mapValues(_.toMap)
      .persist()

    val spaceKeys: Iterable[(Long, Long)] = timeIdxRDD
      .mapValues(_.keySet)
      .reduce({ case ((k1, v1), (k2, v2)) => (k1, v1 ++ v2) })
      ._2

    /*
    * Complete a map with missing keys
     */
    def completeMap(keySet: Seq[(Long, Long)],
                    map: Map[(Long, Long), AttribType]): Seq[AttribType] = {
      for (idxPair <- keySet)
        yield map.getOrElse(idxPair, NAValue)
    }

    def rowFromMap(k: Long, map: Map[(Long, Long), AttribType]): Row = {
      Row.fromSeq(k.toDouble +: completeMap(spaceKeys.toSeq, map))
    }

    val almostDF = timeIdxRDD.map(pair => rowFromMap(pair._1, pair._2))
    timeIdxRDD.unpersist()

    val headers = "time" +: spaceKeys
      .toSeq
      .map({ case (x, y) => "Loc_" + x.toString + "_" + y.toString })
    val schema = StructType(headers.map(x => StructField(x, DoubleType, false)))

    sqlContext.createDataFrame(almostDF, schema)
  }

  def extractSpatialHeaders(timeIndexDF: DataFrame): Seq[(Int, Int)] = {
    timeIndexDF.schema.fieldNames.drop(1) // First column should be time
      .map(_.split("_").drop(1)) // Column names should be in format Loc_xIdx_yIdx
      .map(x => (x(0).toInt, x(1).toInt))
  }

  def pictFromRow(h: Int, w: Int, snapshot: Row, rowNames: Seq[String]): DenseMatrix[Double] ={
    val result = DenseMatrix.zeros[Double](w, h)
    for(i <- rowNames.indices){
      val colName: String = rowNames(i)
      val splitResult = colName.split("_")
      val (x, y): (Int, Int) = (splitResult(1).toInt, splitResult(2).toInt)
      result(y, x) = snapshot.getAs[Double](i + 1) // Coordinates in pictural convention
    }
    result
  }

  def sumOfDF(df: DataFrame, dropFirstColumn: Boolean): Double = {
    if (dropFirstColumn) {
      df.map(_.toSeq.drop(1)
        .reduce(_.asInstanceOf[Double] +
        _.asInstanceOf[Double]))
        .reduce(_.asInstanceOf[Double] + _.asInstanceOf[Double])
        .asInstanceOf[Double]
    } else {
      df.map(_.toSeq.reduce(_.asInstanceOf[Double] +
        _.asInstanceOf[Double]))
        .reduce(_.asInstanceOf[Double] + _.asInstanceOf[Double])
        .asInstanceOf[Double]
    }
  }

  def sumOfArrayRow(collected: Array[Row], dropFirstColumn: Boolean): Double ={
    collected
      .map(_.toSeq.drop(1).reduce(_.asInstanceOf[Double] + _.asInstanceOf[Double]))
      .reduce(_.asInstanceOf[Double] + _.asInstanceOf[Double])
      .asInstanceOf[Double]
  }

}

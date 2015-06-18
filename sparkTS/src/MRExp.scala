/**
 * Created by cusgadmin on 6/9/15.
 */

import GeoTsRDD.LocEvent
import GeoTsRDD.Rasterizer
import GeoTsRDD.Aggregator

import scala.math._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.github.nscala_time.time.Imports._
import java.text.SimpleDateFormat
import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.types._

import scala.reflect._

object MRExp {
  def main(args: Array[String]): Unit ={

    val conf      = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc        = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val tripDataFile = "/users/cusgadmin/traffic_data/new_york_taxi_data/trip_data_1.csv"
    val fareDataFile = "/users/cusgadmin/traffic_data/new_york_taxi_data/trip_fare_1.csv"

    val tripDataDf: DataFrame = sqlContext.load("com.databricks.spark.csv", Map("path" -> tripDataFile, "header" -> "true"))
    val fareDataDf: DataFrame = sqlContext.load("com.databricks.spark.csv", Map("path" -> fareDataFile, "header" -> "true"))

    /*
    val smallTripDataDf = tripDataDf.sample(false, 0.01)
    val smallFareDataDf = fareDataDf.sample(false, 0.01)

    val df = smallTripDataDf.
      join(smallFareDataDf)
      .where(smallTripDataDf
                .col("medallion")
                .equalTo(smallFareDataDf.col("medallion")) &&
              smallTripDataDf
                .col("hack_license")
                .equalTo(smallTripDataDf.col("hack_license")))
    */

    val df = tripDataDf.sample(false, 0.01)
      .select("pickup_longitude", "pickup_latitude", "pickup_datetime",
              "trip_time_in_secs", "trip_distance")

    df.show()
    df.printSchema()

    val format = sc.broadcast(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))

    def convertRowToEvent(row: Row, xIdx: Int, yIdx: Int, tIdx: Int, attribIdx: Int):
      Option[LocEvent[(Double, Double, DateTime), Double]] ={
      try {
        val parsedEvent = new LocEvent[(Double, Double, DateTime), Double](
          (row.getAs[String](xIdx).toDouble,
              row.getAs[String](yIdx).toDouble,
              new DateTime(format.value.parse(row.getAs[String](tIdx)))),
          row.getAs[String](attribIdx).toDouble
        )
        Some(parsedEvent)
      }catch{
        case _ => None
      }
    }

    val rdd = df.map(row => convertRowToEvent(row, 0, 1, 2, 3))

    val filtered_rdd = rdd.filter(!_.isEmpty).map(_.get)

    filtered_rdd.take(100).map(println)

    val x_min: Double = filtered_rdd.map(x => x.getLoc()._1).min() // Need to change that
    val x_max: Double = filtered_rdd.map(_.getLoc()._1).max()
    val y_min: Double = filtered_rdd.map(_.getLoc()._2).min()
    val y_max: Double = filtered_rdd.map(_.getLoc()._2).max()

    val grid_res = (800, 800)
    val span_x  = (x_min - x_max)
    val span_y  = (y_min - y_max)
    val delta_x = span_x / grid_res._1.toDouble
    val delta_y = span_y / grid_res._2.toDouble

    def spatialGrid(loc: (Double, Double, DateTime)): Long={
      val x_idx: Long = floor((loc._1 - x_min) / delta_x).toLong
      val y_idx: Long = floor((loc._2 - y_min) / delta_y).toLong
      x_idx * grid_res._1 + y_idx
    }

    def addAggregator = new Aggregator[Double,(Double, Long)](x=>(x,1),
      {case ((x, n), y) => (x + y, n + 1)},
      {case ((x, n), (y, m)) => (x + y, n + m)}
    )

    def idMapper(pair:Pair[Long, Pair[Double, Long]]) = (pair._1, pair._2._1 / pair._2._2)

    val rasterizer = new Rasterizer[(Double, Double, DateTime), Double, (Double, Long)](spatialGrid, addAggregator, idMapper, filtered_rdd)

    println(rasterizer.getValueAtLoc((-73.937523, 40.758148, new DateTime())))

  }
}

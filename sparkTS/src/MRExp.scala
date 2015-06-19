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

import breeze.linalg._
import breeze.plot._

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

    val df = tripDataDf
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

    println(filtered_rdd.count() + " samples are going to be processed")

    /*
    val x_min: Double = filtered_rdd.map(_.getLoc()._1).min()
    val x_max: Double = filtered_rdd.map(_.getLoc()._1).max()
    val y_min: Double = filtered_rdd.map(_.getLoc()._2).min()
    val y_max: Double = filtered_rdd.map(_.getLoc()._2).max()
    */

    val x_max = -73.95
    val x_min = -73.92
    val y_max = 41.0
    val y_min = 40.5
    val t_min = filtered_rdd.map(_.getLoc()._3).min().getMillis
    val t_max = filtered_rdd.map(_.getLoc()._3).max().getMillis

    val grid_res = (40, 40, 400)
    val span_x  = x_max - x_min
    val span_y  = y_max - y_min
    val span_t  = t_max - t_min
    val delta_x = span_x / grid_res._1.toDouble
    val delta_y = span_y / grid_res._2.toDouble
    val delta_t = span_t / grid_res._3.toDouble

    def spaceTimeGrid(loc: (Double, Double, DateTime)): Long={
      val x_idx: Long = floor((loc._1 - x_min) / delta_x).toLong
      val y_idx: Long = floor((loc._2 - y_min) / delta_y).toLong
      val t_idx: Long = floor((loc._3.getMillis() - t_min) / delta_t).toLong
      x_idx * grid_res._1 * grid_res._2 + y_idx * grid_res._1 + t_idx
    }

    def meanAggregator = new Aggregator[Double,(Double, Long)](
      x=>(x,1),
      {case ((x, n), y) => (x + y, n + 1)},
      {case ((x, n), (y, m)) => (x + y, n + m)}
    )

    def idMapper(pair:Pair[Long, Pair[Double, Long]]) = (pair._1, pair._2._1 / pair._2._2)

    val start_create = java.lang.System.currentTimeMillis()

    val rasterizer = new Rasterizer[
      (Double, Double, DateTime),
      Double,
      (Double, Long)](spaceTimeGrid, meanAggregator, idMapper, filtered_rdd)

    val stop_create = java.lang.System.currentTimeMillis() - start_create

    println("Finished creating rasters, took " + stop_create + " milliseconds")

    /*
    val collected = rasterizer.collectRasters()
    println(collected)
    println(rasterizer.getValueAtLoc((-73.937523, 40.758148, new DateTime())))
    */

    val x_values = linspace(x_min, x_max, grid_res._1)
    val y_values = linspace(y_min, y_max, grid_res._2)
    val t_values = linspace(t_min, t_max, grid_res._3)

    val values = (0 to grid_res._3).map(x => DenseMatrix.zeros[Double](grid_res._1, grid_res._2))

    val value_map = rasterizer.collectAsMap()

    val startPixels = java.lang.System.currentTimeMillis()

    for(i <- 0 to grid_res._1 - 1){
      for(j <- 0 to grid_res._2 - 1){
        for(k <- 0 to grid_res._3 - 1){
          values(k)(i, j) = value_map.getOrElse(spaceTimeGrid((x_values(i), y_values(j), new DateTime(t_values(k).toLong))), 0.0)
        }
      }
    }

    val stopPixels = java.lang.System.currentTimeMillis() - startPixels

    println("Finished creating pixels, took " + stop_create + " milliseconds")

    val f2 = Figure()
    f2.subplot(0) += image(values(0))
    f2.subplot(2,1,1) += image(values(1))
    f2.subplot(2,1,1).title = filtered_rdd.count() + " samples were processed"
    f2.saveas("image.png")

  }
}

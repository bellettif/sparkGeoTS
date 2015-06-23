/**
 * Created by cusgadmin on 6/9/15.
 */

import scala.math._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.sql.{DataFrame, SQLContext, Row}

import com.github.nscala_time.time.Imports._
import org.joda.time.DateTime

import breeze.plot._

import geoTsRDD.Rasterizer
import geoTsRDD.Aggregator
import geoTsRDD.SpatialUtils._

import ioTools.Csv_io


object MRExp {
  def main(args: Array[String]): Unit ={

    // Parameters of query
    val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

    val start_date_str: String = "2013-01-01 00:00:00"
    val start_date: DateTime   = new DateTime(format.parseDateTime(start_date_str))
    val end_date_str: String   = "2013-01-02 00:00:00"
    val end_date: DateTime     = new DateTime(format.parseDateTime(end_date_str))

    val maximumTripTimeInSecs = 2210.26

    val tMin: Long = 1356998497 * 1000
    val tMax: Long = 1357112436 * 1000

    val xMin: Double = -74.010376
    val xMax: Double = -73.782036

    val yMin: Double = 40.639046
    val yMax: Double = 40.81926

    val gridResX: Long = 200
    val gridResY: Long = 200
    val gridResT: Long = 24 * 12

    // Initialize spark context
        val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc    = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val tripDataFile = "/users/cusgadmin/traffic_data/new_york_taxi_data/trip_data_1.csv"
    val fareDataFile = "/users/cusgadmin/traffic_data/new_york_taxi_data/trip_fare_1.csv"

    // Load and filter data
    val tripDataDF: DataFrame = Csv_io.loadFromCsv(tripDataFile, sqlContext, true)

    val df = tripDataDF
      .select("pickup_longitude", "pickup_latitude", "pickup_datetime",
              "trip_time_in_secs", "trip_distance")
      .filter("trip_time_in_secs <=" + maximumTripTimeInSecs.toString)
      .filter("trip_time_in_secs >= 0")
      .filter("pickup_longitude >= " + xMin.toString)
      .filter("pickup_longitude < " + xMax.toString)
      .filter("pickup_latitude >= " + yMin.toString)
      .filter("pickup_latitude < " + yMax.toString)

    val rdd = df.map(row => convertRowToEvent(row, 0, 1, 2, 3, "yyyy-MM-dd HH:mm:ss"))

    val filtered_rdd = rdd
      .filter(_.getLoc()._3 >= start_date)
      .filter(_.getLoc()._3 < end_date)

    println(filtered_rdd.count() + " samples are going to be processed")
    println(filtered_rdd.map(_.getAttrib()).reduce(_ + _) + " total travel time")

    // Compute grid and rasters
    val deltaX = (xMax - xMin) / gridResX.toDouble
    val deltaY = (yMax - yMin) / gridResY.toDouble
    val deltaT = (tMax - tMin).toDouble / gridResT.toDouble

    def spaceTimeGrid(loc: (Double, Double, DateTime)): (Long, Long, Long) = {
      val xIdx: Long = floor((loc._1 - xMin) / deltaX).toLong
      val yIdx: Long = floor((loc._2 - yMin) / deltaY).toLong
      val tIdx: Long = floor((loc._3.getMillis() - tMin) / deltaT).toLong
      (xIdx, yIdx, tIdx)
    }

    def meanAggregator = new Aggregator[Double,(Double, Long)](
      x=>(x, 1),
      {case ((x, n), y) => (x + y, n + 1)},
      {case ((x, n), (y, m)) => (x + y, n + m)}
    )

    def idMapper(pair:Pair[(Long, Long, Long), Pair[Double, Long]]) = (
      pair._1,
      pair._2._1 / pair._2._2.toDouble)

    val rasterizer = new Rasterizer[
      (Double, Double, DateTime),
      (Long, Long, Long),
      Double,
      (Double, Long)](spaceTimeGrid, meanAggregator, idMapper, filtered_rdd)

    // Convert rasters to time series
    val timeIdxDf: DataFrame = toTimeIndexedDf[Double](gridResX.toInt,
      gridResY.toInt,
      0.0,
      rasterizer.rasters,
      sqlContext)

    // Collect and plot a snapshot
    val spaceKeys = extractSpatialHeaders(timeIdxDf)

    println("Sum of DF = " + sumOfDF(timeIdxDf, true).toString)

    val collected: Array[Row] = timeIdxDf
      .sort("time")
      .filter("time = " + timeIdxDf.select("time").head.getAs[String](0))
      .collect()

    val values = pictFromRow(gridResY.toInt, gridResX.toInt, collected(0),
      spaceKeys.map({case (x, y) => "Loc_" + x.toString + "_" + y.toString}).toSeq)

    val f2 = Figure()
    f2.subplot(0) += image(values)
    f2.saveas("image.png")

  }
}

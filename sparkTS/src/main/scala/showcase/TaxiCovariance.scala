package main.scala.showcase

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import main.scala.ioTools.ReadCsv
import main.scala.overlapping.analytics.CrossCovariance
import main.scala.overlapping.containers._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object TaxiCovariance {

   def main(args: Array[String]): Unit = {

     /*
     ##########################################

       Exploratory analysis

     ##########################################
      */

     val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
     implicit val sc = new SparkContext(conf)


     val inSampleFilePath = "/users/cusgadmin/traffic_data/new_york_taxi_data/taxi_earnings_resampled/all.csv"

     val (inSampleDataHD, _, nSamples) = ReadCsv.TS(inSampleFilePath)(sc)

     val inSampleData = inSampleDataHD.mapValues(v => v(60 until 120))

     val d = 60
     val deltaTMillis = 5 * 60L * 1000L
     val deltaT = new DateTime(deltaTMillis) // 5 minutes
     val paddingMillis = new DateTime(deltaTMillis * 100)
     val nPartitions   = 8
     val config = new SingleAxisVectTSConfig(nSamples, deltaT, paddingMillis, paddingMillis, d)

     println(nSamples + " samples")
     println(d + " dimensions")
     println()

     val (timeSeries, _) = SingleAxisVectTS(nPartitions, config, inSampleData)

     val covariances = CrossCovariance(timeSeries, 6)

     covariances.foreach(x => {println(x); println()})

   }
 }
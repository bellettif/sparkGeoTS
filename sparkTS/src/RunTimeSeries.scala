/**
 * Created by cusgadmin on 6/9/15.
 */

import TsUtils.HF_estimators.HayashiYoshida
import TsUtils.Models.{AutoCorrelation, CrossCovariance, ARModel, MAModel, ARMAModel}
import TsUtils.{TimeSeries, TestUtils}
import breeze.numerics.sqrt
import groovy.sql.Sql

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.rdd.{RDD, OrderedRDDFunctions}

import org.joda.time.DateTime

import breeze.linalg._

object RunTimeSeries {

  def main(args: Array[String]): Unit ={

    val nColumns = 10
    val nSamples = 10000L
    val effectiveLag = 40L
    val deltaTMillis = 1L

    val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc    = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //val rawTsRDD = TestUtils.getAR2TsRDD(0.5, 0.2, nColumns, nSamples, deltaTMillis, sc)
    //val rawTsRDD = TestUtils.getAR1TsRDD(0.70, nColumns, nSamples, deltaTMillis, sc)
    val rawTsRDD = TestUtils.getMA1TsRDD(0.67, nColumns, nSamples.toInt, deltaTMillis, sc)
    //val rawTsRDD = TestUtils.getRandomRawTsRDD(nColumns, nSamples.toInt, deltaTMillis, sc)

    val temp = rawTsRDD.collect

    val timeSeries = TimeSeries[Array[Any], Double](
      rawTsRDD,
      nColumns,
      x => (x.head.asInstanceOf[DateTime], x.drop(1).map(_.asInstanceOf[Double])),
      sc,
      effectiveLag
    )

    /*
    #############################################

              GLOBAL OPERATIONS

    #############################################
     */

    val HYEstimator = new HayashiYoshida(timeSeries, timeSeries)

    val (variation1, variation2, covariation) = HYEstimator.computeCrossFoldHY[Double](
    {case ((x1, x2), (y1, y2)) => (y2 - y1) * (x2 - x1)},
    {case (x1, x2) => (x1 - x2) * (x1 - x2)},
    {case (x1, x2) => (x1 - x2) * (x1 - x2)},
    _ + _,
    0.0
    )(0, 1, 0L)

    println(covariation / sqrt(variation1 * variation2))
    println("Done")


    /*
    This will compute the autocorrelation (until rank 5 included) of each column of the time series
     */
    val autoCor = new AutoCorrelation(5)
    val startAuto = java.lang.System.currentTimeMillis()
    val acf = autoCor.estimate(timeSeries)
    val timeSpentAuto = java.lang.System.currentTimeMillis() - startAuto
    println(timeSpentAuto)

    /*
    This will compute the cross-correlation (between columns) of the time series
     */
    val crossCov = new CrossCovariance(5)
    val startCross = java.lang.System.currentTimeMillis()
    val crossAcf = crossCov.estimate(timeSeries)
    val timeSpentCross = java.lang.System.currentTimeMillis() - startCross
    println(timeSpentCross)

    /*
    This will calibrate an AR model (one per column) on the time series
     */
    val AR = new ARModel(5)
    val startAR = java.lang.System.currentTimeMillis()
    val ARcoefs = AR.estimate(timeSeries)
    val timeSpentAR = java.lang.System.currentTimeMillis() - startAR
    println(timeSpentAR)

    /*
    This will calibrate a MA model (one per column) on the time series
     */
    var MA = new MAModel(5)
    val startMA = java.lang.System.currentTimeMillis()
    val MAcoefs = MA.estimate(timeSeries)
    val timeSpentMA = java.lang.System.currentTimeMillis() - startMA
    println(timeSpentMA)

    /*
    This will calibrate an ARMA model (one per column) on the time series
     */
    var ARMA = new ARMAModel(5, 5)
    val startARMA = java.lang.System.currentTimeMillis()
    val ARMAcoefs = ARMA.estimate(timeSeries)
    val timeSpentARMA = java.lang.System.currentTimeMillis() - startARMA
    println(timeSpentARMA)

    /*
    ############################################

              WINDOWED OPERATIONS

    ###########################################
     */

    def secondSlicer(t1 : DateTime, t2: DateTime): Boolean ={
      t1.secondOfDay() != t2.secondOfDay()
    }

    def f(ts: Array[Array[Double]]): Array[Double] = {
      // Return a column based average of the table
      ts.map(x => x.sum)
    }

    /*
    This will compute the windowed sum of each column of the timeseries (each window spans a second)
     */
    val windowedSums              = timeSeries.windowApply(f, secondSlicer).collect

    /*
    This will compute the autocorrelation of each column of the timeseries (each window spans a second)
     */
    val windowedAutoCorrelations  = timeSeries.windowApply(autoCor.estimate, secondSlicer).collect

    /*
    This will compute the cross correlation (between columns) of the time series (each window spans a second)
     */
    val windowedCrossCorrelations = timeSeries.windowApply(crossCov.estimate, secondSlicer).collect

    /*
    This will compute a windowed AR calibration
     */
    val windowedAR = timeSeries.windowApply(AR.estimate, secondSlicer).collect

    /*
    This will compute a windowed MA calibration
     */
    val windowedMA = timeSeries.windowApply(MA.estimate, secondSlicer).collect

  }
}

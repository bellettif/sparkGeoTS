/**
 * Created by cusgadmin on 6/9/15.
 */

import TsUtils.Models.{AutoCorrelation, CrossCovariance, DurbinLevinsonAR, InnovationAlgoMA}
import TsUtils.{TimeSeries, TestUtils}

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.rdd.{RDD, OrderedRDDFunctions}

import org.joda.time.DateTime

import breeze.linalg._

object TestTsDataFrame {

  def main(args: Array[String]): Unit ={

    val nColumns = 10
    val nSamples = 10000

    val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc    = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //val rawTsRDD = TestUtils.getAR2TsRDD(0.5, 0.2, nColumns, nSamples, sc)
    //val rawTsRDD = TestUtils.getAR1TsRDD(0.70, nColumns, nSamples, sc)
    val rawTsRDD = TestUtils.getMA1TsRDD(0.8, nColumns, nSamples, sc)

    val timeSeries = new TimeSeries[Array[Any], Double](rawTsRDD,
      x => (x.head.asInstanceOf[DateTime], x.drop(1).map(_.asInstanceOf[Double])),
      sc,
      Some(20)
    )

    /*
    #############################################

              GLOBAL OPERATIONS

    #############################################
     */


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
    val DLAR = new DurbinLevinsonAR(5)
    val startAR = java.lang.System.currentTimeMillis()
    val ARcoefs = DLAR.estimate(timeSeries)
    val timeSpentAR = java.lang.System.currentTimeMillis() - startAR
    println(timeSpentAR)

    /*
    This will calibrate a MA model (onde per column) on the time series
     */
    var IAMA = new InnovationAlgoMA(5)
    val startMA = java.lang.System.currentTimeMillis()
    val MAcoefs = IAMA.estimate(timeSeries)
    val timeSpentMA = java.lang.System.currentTimeMillis() - startMA
    println(timeSpentMA)

    /*
    ############################################

              WINDOWED OPERATIONS

    ###########################################
     */

    def secondSlicer(t1 : DateTime, t2: DateTime): Boolean ={
      t1.secondOfDay() != t2.secondOfDay()
    }

    def f(ts: Seq[Array[Double]]): Iterator[Double] = {
      // Return a column based average of the table
      ts.map(x => x.sum).toIterator
    }

    /*
    This will compute the windowed sum of each column of the timeseries (each window spans a second)
     */
    val windowedSums              = timeSeries.applyBy(f, secondSlicer).collectAsMap

    /*
    This will compute the autocorrelation of each column of the timeseries (each window spans a second)
     */
    val windowedAutoCorrelations  = timeSeries.applyBy(autoCor.estimate, secondSlicer).collectAsMap

    /*
    This will compute the cross correlation (between columns) of the time series (each window spans a second)
     */
    val windowedCrossCorrelations = timeSeries.applyBy(crossCov.estimate, secondSlicer).collectAsMap

    /*
    This will compute a windowed AR calibration
     */
    val windowedAR = timeSeries.applyBy(DLAR.estimate, secondSlicer).collectAsMap

    /*
    This will compute a windowed MA calibration
     */
    val windowedMA = timeSeries.applyBy(IAMA.estimate, secondSlicer).collectAsMap


    println("Done")

  }
}

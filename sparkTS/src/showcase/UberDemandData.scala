package showcase

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.plot._

import ioTools.ReadCsv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import overlapping._
import containers._
import timeSeries._

object UberDemandData {

  def main(args: Array[String]): Unit = {

    implicit def signedDistMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    implicit def signedDistLong = (t1: Long, t2: Long) => (t2 - t1).toDouble

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)

    /*
    ##########################################

      In sample analysis

    ##########################################
     */

    val inSampleFilePath = "/users/cusgadmin/traffic_data/new_york_taxi_data/taxi_earnings_resampled/all.csv"

    val (inSampleData, d, nSamples) = ReadCsv.TS(inSampleFilePath)

    val deltaTMillis = 5 * 60L * 1000L // 5 minutes
    val paddingMillis  = deltaTMillis * 100L
    val nPartitions   = 8
    implicit val config = TSConfig(deltaTMillis, d, nSamples, paddingMillis.toDouble)

    println(nSamples + " samples")
    println(d + " dimensions")
    println()

    val (rawTimeSeries: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, inSampleData)

    /*
    ############################################

      Get rid of seasonality

    ############################################
     */

    def hashFunction(x: TSInstant): Int = {
      (x.timestamp.getDayOfWeek - 1) * 24 * 60 / 5 + (x.timestamp.getMinuteOfDay - 1) / 5
    }
    val matrixMeanProfile = DenseMatrix.zeros[Double](7 * 24 * 12, d)

    val meanProfile = MeanProfileEstimator(rawTimeSeries, hashFunction)

    for(k <- meanProfile.keys){
      matrixMeanProfile(k, ::) := meanProfile(k).t
    }

    PlotTS.showProfile(matrixMeanProfile, "Weekly demand profile")

    val noSeason = MeanProfileEstimator.removeSeason(inSampleData, hashFunction, meanProfile)

    val (timeSeriesRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, noSeason)

    /*
    ############################################

      Usual analysis

    ############################################
     */

    val chosenRegions = Array(42, 137, 243)

    PlotTS(timeSeriesRDD, "In Sample Data", Some(chosenRegions))

    /*
    ################################

    Correlation analysis

    ###############################
     */
    val (correlations, _) = CrossCorrelation(timeSeriesRDD, 6)
    PlotTS.showModel(correlations, "Cross correlation")
    //correlations.foreach(x => {println(x); println})

    val (partialCorrelations, _) = PartialCrossCorrelation(timeSeriesRDD,6)
    PlotTS.showModel(partialCorrelations, "Partial cross correlation")
    partialCorrelations.foreach(x => {println(x); println})

    val chosenP = 3

    /*
    ################################

    Monovariate analysis

    ################################
     */

    val mean = MeanEstimator(timeSeriesRDD)
    val vectorsAR = ARModel(timeSeriesRDD, chosenP, Some(mean)).map(_.covariation)
    val residualsAR = ARPredictor(timeSeriesRDD, vectorsAR, Some(mean))
    val residualSecondMomentAR = SecondMomentEstimator(residualsAR)

    PlotTS.showUnivModel(vectorsAR, "Monovariate parameter estimates")
    PlotTS(residualsAR, "Monovariate AR residual error", Some(chosenRegions))

    println("AR in sample error = " + trace(residualSecondMomentAR))

    /*
    ##################################

    Multivariate analysis

    ##################################
     */

    val (estVARMatrices, _) = VARModel(timeSeriesRDD, chosenP)

    PlotTS.showModel(estVARMatrices, "Multivariate parameter estimates")

    val residualVAR = VARPredictor(timeSeriesRDD, estVARMatrices, Some(mean))

    val residualSecondMomentVAR = SecondMomentEstimator(residualVAR)

    PlotTS.showCovariance(residualSecondMomentAR, "Monovariate residual covariance")
    PlotTS.showCovariance(residualSecondMomentVAR, "Multivariate residual covariance")

    println("Frequentist VAR residuals")
    println(trace(residualSecondMomentVAR))
    println()



  }
}
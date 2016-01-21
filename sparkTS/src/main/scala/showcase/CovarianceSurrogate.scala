package main.scala.showcase

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg.DenseVector
import breeze.stats.distributions.Gaussian
import main.scala.overlapping.analytics.{CrossCorrelation, CrossCovariance}
import main.scala.overlapping.containers._
import main.scala.overlapping.dataGenerators.Surrogate
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object CovarianceSurrogate {

   def main(args: Array[String]): Unit = {

     /*
     ##########################################

       Exploratory analysis

     ##########################################
      */

     val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
     implicit val sc = new SparkContext(conf)

     val nSamples = 8000L
     val d = 3
     val deltaTMillis = 1L
     val deltaT = new DateTime(deltaTMillis) // 5 minutes
     val paddingMillis = new DateTime(deltaTMillis * 10)
     val nPartitions   = 8
     val config = new SingleAxisVectTSConfig(nSamples, deltaT, paddingMillis, paddingMillis, d)

     val inSampleData = Surrogate.generateWhiteNoise(
       d,
       nSamples.toInt,
       deltaT,
       Gaussian(0.0, 4.0), DenseVector.ones[Double](d),
       sc)

     println(nSamples + " samples")
     println(d + " dimensions")
     println()

     val (timeSeries, _) = SingleAxisVectTS(nPartitions, config, inSampleData)

     println("Covariance estimate")
     val covariances = CrossCovariance(timeSeries, 3)
     covariances.foreach(x => {println(x); println()})

     println("Correlation estimate")
     val correlations = CrossCorrelation(timeSeries, 3)
     correlations.foreach(x => {println(x); println()})


   }
 }
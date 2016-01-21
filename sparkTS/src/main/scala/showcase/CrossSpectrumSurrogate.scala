package main.scala.showcase

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.numerics.{atan, abs}
import breeze.stats.distributions.Gaussian
import main.scala.overlapping.analytics.{CrossSpectrum, CrossCorrelation, CrossCovariance}
import main.scala.overlapping.containers._
import main.scala.overlapping.dataGenerators.Surrogate
import main.scala.plotting.PlotCross._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object CrossSpectrumSurrogate {

   def main(args: Array[String]): Unit = {

     /*
     ##########################################

       Exploratory analysis

     ##########################################
      */

     val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
     implicit val sc = new SparkContext(conf)

     val nSamples = 100000L
     val d = 3
     val deltaTMillis = 1L
     val deltaT = new DateTime(deltaTMillis) // 5 minutes
     val paddingMillis = new DateTime(deltaTMillis * 1000)
     val nPartitions   = 8
     val config = new SingleAxisVectTSConfig(nSamples, deltaT, paddingMillis, paddingMillis, d)

     val p = 4

     val actualParams: Array[DenseMatrix[Double]] = Array.fill(p){DenseMatrix.rand[Double](d, d) * 0.4 - (DenseMatrix.ones[Double](d, d) * 0.20)}

     val inSampleData = Surrogate.generateVAR(
       actualParams,
       d,
       nSamples.toInt,
       deltaT,
       Gaussian(0.0, 0.5),
       DenseVector.ones[Double](d),
       sc)

     println(nSamples + " samples")
     println(d + " dimensions")
     println()

     val (timeSeries, _) = SingleAxisVectTS(nPartitions, config, inSampleData)

     println("Cross spectrum estimate")
     val crossSpectra = CrossSpectrum(timeSeries, 500)

     println("Cross spectrum magnitude estimate")
     val crossSpectraMagn = crossSpectra.map(x => x.map(abs(_)))
     val crossSpectraPhases = crossSpectra.map(_.map(_.map(x => atan(x.imag / x.real))))

     showCrossSpectrum(crossSpectraMagn, Some("Power spectrum"))
     showCrossSpectrum(crossSpectraPhases, Some("Phase"))


   }
 }
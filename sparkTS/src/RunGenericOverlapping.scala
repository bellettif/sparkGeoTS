/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg.{DenseMatrix, sum}
import breeze.numerics.sqrt
import breeze.stats.distributions.{Gaussian, Uniform}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import overlapping.IntervalSize
import overlapping.containers.block.{SingleAxisBlock, IntervalSampler}
import overlapping.io.SingleAxisBlockRDD
import overlapping.models.secondOrder._
import overlapping.surrogateData.{TSInstant, IndividualRecords}

import scala.math.Ordering

object RunGenericOverlapping {

  def main(args: Array[String]): Unit ={

    val nColumns      = 2
    val nSamples      = 1000000L
    val paddingMillis = 1000L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc    = new SparkContext(conf)

    //val rawTS = IndividualRecords.generateWhiteNoise(nColumns, nSamples.toInt, deltaTMillis, sc)
    //val rawTS = IndividualRecords.generateOnes(nColumns, nSamples.toInt, deltaTMillis, sc)
    //val rawTS = IndividualRecords.generateAR1(0.9, nColumns, nSamples.toInt, deltaTMillis, sc)
    //val rawTS = IndividualRecords.generateMA(Array(0.4, 0.3, 0.1, 0.05), nColumns, nSamples.toInt, deltaTMillis, sc)
    //val rawTS = IndividualRecords.generateAR2(0.6, 0.2, nColumns, nSamples.toInt, deltaTMillis, sc)
    //val rawTS = IndividualRecords.generateMA1(0.6, nColumns, nSamples.toInt, deltaTMillis, sc)

    val rawTS = IndividualRecords.generateVAR(
      Array(DenseMatrix((0.25, 0.15), (-0.15, 0.20)), DenseMatrix((0.06, 0.03), (0.07, -0.09))),
      //Array(DenseMatrix.eye[Double](nColumns) :* 0.10, DenseMatrix.eye[Double](nColumns) :* 0.05),
      nColumns, nSamples.toInt, deltaTMillis,
      Gaussian(0.0, 2.0),
      sc);

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])], intervals: Array[(TSInstant, TSInstant)]) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    /*
    val crossCov = new CrossCovariance[TSInstant](IntervalSize(5, 5), 5)
    val result = crossCov.estimate(overlappingRDD)
    */

    /*
    val autoCov = new AutoCovariances[TSInstant](5.0, 5)
    val result = autoCov.estimate(overlappingRDD)
    */

    /*
    println("Results of AR univariate frequentist estimator")

    val AREstimator = new ARModel[TSInstant](1.0, 5)
    AREstimator
      .estimate(overlappingRDD)
      .foreach(println)

    println()

    println("Results of MA univariate frequentist estimator")

    val MAEstimator = new MAModel[TSInstant](1.0, 5)
    MAEstimator
      .estimate(overlappingRDD)
      .foreach(println)

    println()

    println("Results of ARMA univariate frequentist estimator")

    val ARMAEstimator = new ARMAModel[TSInstant](1.0, 2, 2)
    ARMAEstimator
      .estimate(overlappingRDD)
      .foreach(println)

    println()
    */

    println("Results of cross covariance multivariate frequentist estimator")

    val VAREstimator = new VARModel[TSInstant](1.0, 3)
    val (coeffMatrices, noiseVariance) = VAREstimator
      .estimate(overlappingRDD)

    println("Cross covariance:")
    coeffMatrices.foreach(println)
    println(noiseVariance)

    println()



    /*
    val cutPredicate = (x: TSInstant, y: TSInstant) => x.timestamp.secondOfDay() != y.timestamp.secondOfDay()

    val slidingResult = overlappingRDD
      .mapValues(x => x.slicingWindow(Array(cutPredicate))(MAEstimator.estimate).toArray)
      .collect

    slidingResult.foreach(println)
    */

  }
}
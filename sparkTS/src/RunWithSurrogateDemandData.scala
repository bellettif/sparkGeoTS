/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.stats.distributions.Gaussian
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import overlapping.containers.block.SingleAxisBlock
import overlapping.io.SingleAxisBlockRDD
import overlapping.models.firstOrder.{MeanEstimator, SecondMomentEstimator}
import overlapping.models.secondOrder.multivariate.VARPredictor
import overlapping.models.secondOrder.multivariate.frequentistEstimators.{CrossCovariance, VARModel}
import overlapping.models.secondOrder.univariate.{AutoCovariances, ARModel, ARPredictor}
import overlapping.surrogateData.{IndividualRecords, TSInstant}

import scala.math.Ordering

object RunWithSurrogateDemandData {

  def main(args: Array[String]): Unit = {

    val filePath = "/users/cusgadmin/traffic_data/uber-ny/uber_spatial_bins_20x20_merged.csv"

    val nColumns = 3
    val nSamples = 100000L
    val paddingMillis = 100L
    val deltaTMillis = 1L
    val nPartitions = 8

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /*
    val ARcoeffs = Array(DenseMatrix((0.25, 0.0, -0.01), (0.15, -0.30, 0.04), (0.0, -0.15, 0.35)),
      DenseMatrix((0.06, 0.03, 0.0), (0.0, 0.07, -0.09), (0.0, 0.0, 0.07)))
    */

    val ARcoeffs = Array(DenseMatrix((0.25, 0.0, 0.0), (0.0, -0.30, 0.00), (0.0, 0.0, 0.35)),
      DenseMatrix((0.05, 0.00, 0.0), (0.0, 0.07, 0.0), (0.0, 0.0, -0.07)))


    val rawTS = IndividualRecords.generateVAR(
      ARcoeffs,
      nColumns, nSamples.toInt, deltaTMillis,
      Gaussian(0.0, 1.0),
      DenseVector(1.0, 1.0, 1.0),
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    /*
     Estimate process' mean
     */
    val meanEstimator = new MeanEstimator[TSInstant]()
    val secondMomentEstimator = new SecondMomentEstimator[TSInstant]()

    val mean = meanEstimator.estimate(overlappingRDD)

    /*
    Monovariate analysis
     */
    val p = 2

    val autoCovEstimator = new AutoCovariances[TSInstant](deltaTMillis, p, nColumns, sc.broadcast(mean))
    val autocovariances = autoCovEstimator.estimate(overlappingRDD)

    autocovariances.foreach(x => {println(x.covariation); println(x.variation); println()})

    /*
    val freqAREstimator = new ARModel[TSInstant](deltaTMillis, p, nColumns, sc.broadcast(mean))
    val vectorsAR = freqAREstimator.estimate(overlappingRDD)

    vectorsAR.foreach(x => {println(x.covariation); println(x.variation); println()})

    val predictorAR = new ARPredictor[TSInstant](
      deltaTMillis,
      1,
      nColumns,
      sc.broadcast(mean),
      sc.broadcast(vectorsAR.map(x => x.covariation)))

    val predictionsAR = predictorAR.predictAll(overlappingRDD)
    val residualsAR = predictorAR.residualAll(overlappingRDD)
    val residualMeanAR = meanEstimator.estimate(residualsAR)
    val residualSecondMomentAR = secondMomentEstimator.estimate(residualsAR)

    println(residualMeanAR)
    println(trace(residualSecondMomentAR))
    */

    /*
    Multivariate analysis
     */

    val crossCovarianceEstimator = new CrossCovariance[TSInstant](deltaTMillis, p, nColumns, sc.broadcast(mean))
    val (crossCovariances, _) = crossCovarianceEstimator.estimate(overlappingRDD)

    crossCovariances.foreach(x => {println(x); println()})

    /*
    val freqVAREstimator = new VARModel[TSInstant](deltaTMillis, p, nColumns, sc.broadcast(mean))
    val (freqVARmatrices, _) = freqVAREstimator.estimate(overlappingRDD)

    freqVARmatrices.foreach(x => {println(x); println()})

    val predictorVAR = new VARPredictor[TSInstant](
      deltaTMillis,
      1,
      nColumns,
      sc.broadcast(mean),
      sc.broadcast(freqVARmatrices))

    val predictionsVAR = predictorVAR.predictAll(overlappingRDD)
    val residualsVAR = predictorVAR.residualAll(overlappingRDD)
    val residualMeanVAR = meanEstimator.estimate(residualsVAR)
    val residualSecondMomentVAR = secondMomentEstimator.estimate(residualsVAR)

    println(residualMeanVAR)
    println(trace(residualSecondMomentVAR))

    */

  }
}
/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.plot.{Figure, image}
import breeze.stats.distributions.Gaussian
import ioTools.ReadCsv
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import overlapping.containers.block.SingleAxisBlock
import overlapping.io.SingleAxisBlockRDD
import overlapping.models.firstOrder.{SecondMomentEstimator, MeanEstimator}
import overlapping.models.secondOrder.multivariate.VARPredictor
import overlapping.models.secondOrder.multivariate.frequentistEstimators.{VARModel, CrossCovariance}
import overlapping.models.secondOrder.univariate.{ARPredictor, ARModel}
import overlapping.surrogateData.{IndividualRecords, TSInstant}

import scala.math.Ordering

object RunWithUberDemandData {

  def main(args: Array[String]): Unit = {

    val filePath = "/users/cusgadmin/traffic_data/uber-ny/uber_spatial_bins_20x20_merged.csv"

    val data = ReadCsv(filePath, 0, "yyyy-MM-dd HH:mm:ss", true)

    val nColumns = data.head._2.length
    val nSamples = data.length
    val paddingMillis = 6000000L // 100 minutes
    val deltaTMillis = 60000L // 1 minute
    val nPartitions = 8

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawTS = sc.parallelize(data)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], intervals: Array[(TSInstant, TSInstant)]) =
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
    val p = 1
    val freqAREstimator = new ARModel[TSInstant](deltaTMillis, 1, nColumns, sc.broadcast(mean))
    val vectorsAR = freqAREstimator.estimate(overlappingRDD)

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

    /*
    Multivariate analysis
     */
    val freqVAREstimator = new VARModel[TSInstant](deltaTMillis, 1, nColumns, sc.broadcast(mean))
    val (freqVARmatrices, _) = freqVAREstimator.estimate(overlappingRDD)

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


  }
}
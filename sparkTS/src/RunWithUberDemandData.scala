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
    val mean = meanEstimator.estimate(overlappingRDD)

    /*
    Estimate process' AR characteristics by frequentist method
     */
    val p = 1
    val freqAREstimator = new VARModel[TSInstant](deltaTMillis, 1, nColumns, sc.broadcast(mean))
    val (freqARmatrices, covMatrix) = freqAREstimator.estimate(overlappingRDD)

    val f1 = Figure()
    for (i <- freqARmatrices.indices){
      f1.subplot(i) += image(freqARmatrices(i))
    }
    f1.saveas("AR_1_uber_matrix.png")

    freqARmatrices.foreach(x => {println(x); println()})

    val predictor = new VARPredictor[TSInstant](
      deltaTMillis,
      1,
      nColumns,
      sc.broadcast(mean),
      sc.broadcast(freqARmatrices))

    println("Starting predictions")
    val startTime1 = System.currentTimeMillis()
    val predictions = predictor.predictAll(overlappingRDD)
    println("Done after " + (System.currentTimeMillis() - startTime1) + " milliseconds")

    println("Computing residuals")
    val startTime2 = System.currentTimeMillis()
    val residuals = predictor.residualAll(overlappingRDD)
    println("Done after " + (System.currentTimeMillis() - startTime2) + " milliseconds")

    println("Computing mean residual")
    val startTime3 = System.currentTimeMillis()
    val residualMean = meanEstimator.estimate(residuals)
    println("Done after " + (System.currentTimeMillis() - startTime3) + " milliseconds")
    println(residualMean)

    val secondMomentEstimator = new SecondMomentEstimator[TSInstant]()

    println("Computing mean squared residual")
    val startTime4 = System.currentTimeMillis()
    val residualSecondMoment = secondMomentEstimator.estimate(residuals)
    println("Done after " + (System.currentTimeMillis() - startTime4) + " milliseconds")
    println(residualSecondMoment)

    println(residualMean)
    println(residualSecondMoment)

    val f2 = Figure()
    f2.subplot(0) += image(residualSecondMoment)
    f2.saveas("sigma_eps_uber.png")

    println()

  }
}
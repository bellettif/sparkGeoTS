/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.stats.distributions.Gaussian
import ioTools.ReadCsv
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import overlapping.containers.block.SingleAxisBlock
import overlapping.io.SingleAxisBlockRDD
import overlapping.models.firstOrder.MeanEstimator
import overlapping.models.secondOrder.{CrossCovariance, LateralSplitVARGradientDescent}
import overlapping.surrogateData.{IndividualRecords, TSInstant}

import scala.math.Ordering

object RunWithUberDemandData {

  def main(args: Array[String]): Unit ={

    val filePath = "/users/cusgadmin/traffic_data/uber-ny/uber_spatial_bins_20x20_merged.csv"

    val data = ReadCsv(filePath, 0, "yyyy-MM-dd HH:mm:ss", true)

    val nColumns      = data.head._2.length
    val nSamples      = data.length
    val paddingMillis = 6000000L // 100 minutes
    val deltaTMillis  = 60000L // 1 minute
    val nPartitions   = 8

    val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc    = new SparkContext(conf)

    val rawTS = sc.parallelize(data)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], intervals: Array[(TSInstant, TSInstant)]) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    println("Results of cross cov frequentist estimator")

    val meanEstimator = new MeanEstimator[TSInstant]()

    val mean = meanEstimator.estimate(overlappingRDD)

    val centeredTimeSeries = overlappingRDD.mapValues(_.map({case (_, x) => x - mean}))

    //println(mean)

    val crossCovEstimator = new CrossCovariance[TSInstant](deltaTMillis, 1)
    val (crossCovMatrices, covMatrix) = crossCovEstimator
      .estimate(centeredTimeSeries)

    //crossCovMatrices.foreach(x=> {println(x); println()})

    val svd.SVD(_, s, _) = svd(covMatrix)

    println()

    println(covMatrix(0 until 10, 0 until 10))

  }
}
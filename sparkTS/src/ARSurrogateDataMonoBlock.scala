/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.plot._
import breeze.stats.distributions.Gaussian
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import overlapping.containers.block.SingleAxisBlock
import overlapping.io.SingleAxisBlockRDD
import overlapping.models.firstOrder.MeanEstimator
import overlapping.models.secondOrder.multivariate.bayesianEstimators.{AutoregressiveGradient, AutoregressiveLoss, VARGradientDescent}
import overlapping.models.secondOrder.multivariate.frequentistEstimators._
import overlapping.surrogateData.{IndividualRecords, TSInstant}

import scala.math.Ordering

object ARSurrogateDataMonoBlock {

  def main(args: Array[String]): Unit ={

    val nColumns      = 3
    val nSamples      = 10000L
    val paddingMillis = 1000L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc    = new SparkContext(conf)

    val ARcoeffs = Array(DenseMatrix((0.25, 0.0, -0.01), (0.15, -0.30, 0.04), (0.0, -0.15, 0.35)),
      DenseMatrix((0.06, 0.03, 0.0), (0.0, 0.07, -0.09), (0.0, 0.0, 0.07)))

    val rawTS = IndividualRecords.generateVAR(
      ARcoeffs,
      nColumns, nSamples.toInt, deltaTMillis,
      Gaussian(0.0, 1.0),
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], intervals: Array[(TSInstant, TSInstant)]) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val p = ARcoeffs.length

    val meanEstimator = new MeanEstimator[TSInstant]()
    val mean = meanEstimator.estimate(overlappingRDD)

    println("Results of AR multivariate frequentist estimator")

    println("Mean = ")
    println(mean)

    val VAREstimator = new VARModel[TSInstant](1.0, p, nColumns, mean)
    val (coeffMatricesAR, noiseVarianceAR) = VAREstimator
      .estimate(overlappingRDD)

    println("AR estimated model:")
    coeffMatricesAR.foreach(x=> {println(x); println()})
    println(noiseVarianceAR)
    println()

    val predictor = new VARPredictor[TSInstant](deltaTMillis, 1, nColumns, mean, coeffMatricesAR)

    val predictions = predictor.predictAll(overlappingRDD)

    println(predictions.map(_._2._2).reduce(_ + _) / predictions.count.toDouble)

    val sigmaEps = predictions.map({case (k, (_, e)) => e * e.t}).reduce(_ + _) / predictions.count.toDouble

    val f2 = Figure()
    f2.subplot(0) += image(sigmaEps)
    f2.saveas("image.png")

    println()

  }
}
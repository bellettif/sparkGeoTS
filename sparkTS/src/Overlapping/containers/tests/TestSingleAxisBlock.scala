package overlapping.containers.tests

/**
 * Created by Francois Belletti on 8/17/15.
 */


import breeze.linalg.sum
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime
import org.scalatest.{Matchers, FlatSpec}
import overlapping.containers.block.SingleAxisBlock
import overlapping.io.SingleAxisBlockRDD
import overlapping.surrogateData.{TSInstant, IndividualRecords}

import scala.math.Ordering

/**
 * Created by Francois Belletti on 8/4/15.
 */
class TestSingleAxisBlock extends FlatSpec with Matchers{

  val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
  val sc    = new SparkContext(conf)

  "A SingleAxisBlock" should "conserve the sum of its elements when partitioning" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val rawTS = IndividualRecords.generateWhiteNoise(nColumns, nSamples.toInt, deltaTMillis, sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])] =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val partitionedSum = overlappingRDD
      .mapValues(v => v.map({case(k, v) => sum(v)}).reduce(_ + _)).map(_._2).reduce(_ + _)

    val directSum = rawTS.map(_._2.sum).reduce(_ + _)

    partitionedSum should be (directSum +- 0.000001)

  }

  it should " return the sum of samples when data is filled with 1.0 value" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val rawTS = IndividualRecords.generateOnes(nColumns, nSamples.toInt, deltaTMillis, sc)

    val temp = rawTS.collect()

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])] =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val partitionedSum = overlappingRDD
      .mapValues(v => v.map({case(k, v) => sum(v)}).reduce(_ + _)).map(_._2).reduce(_ + _)

    partitionedSum should be (nSamples.toDouble * nColumns.toDouble +- 0.000001)

  }

  it should "conserve the sum of its elements when partitioning when deltaT != 1" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 3L
    val nPartitions   = 8

    val rawTS = IndividualRecords.generateWhiteNoise(nColumns, nSamples.toInt, deltaTMillis, sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])] =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val partitionedSum = overlappingRDD
      .mapValues(v => v.map({case(k, v) => sum(v)}).reduce(_ + _)).map(_._2).reduce(_ + _)

    val directSum = rawTS.map(_._2.sum).reduce(_ + _)

    partitionedSum should be (directSum +- 0.000001)

  }

  it should " return the sum of samples when data is filled with 1.0 value and deltaT != 1" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 4L
    val nPartitions   = 8

    val rawTS = IndividualRecords.generateOnes(nColumns, nSamples.toInt, deltaTMillis, sc)

    val temp = rawTS.collect()

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])] =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val partitionedSum = overlappingRDD
      .mapValues(v => v.map({case(k, v) => sum(v)}).reduce(_ + _)).map(_._2).reduce(_ + _)

    partitionedSum should be (nSamples.toDouble * nColumns.toDouble +- 0.000001)

  }

}
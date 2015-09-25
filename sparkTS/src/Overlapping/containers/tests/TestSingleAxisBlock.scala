package overlapping.containers.tests

/**
 * Created by Francois Belletti on 8/17/15.
 */

import breeze.stats.distributions.Uniform
import overlapping.IntervalSize

import scala.math._
import org.apache.spark.rdd.RDD
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


  "A SingleAxisBlocks" should " properly map elements" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 4L
    val nPartitions   = 8

    val rawTS = IndividualRecords.generateWhiteNoise(nColumns, nSamples.toInt, deltaTMillis,
      Uniform(-0.5, 0.5),
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val nonOverlappingSeqs = overlappingRDD
      .mapValues(v => v.map({case(k, v_) => v_.map(_ * 2.0)}))
      .mapValues(_.toArray)

    val transformedData = nonOverlappingSeqs
      .collect()
      .map(_._2.toList)
      .foldRight(List[(TSInstant, Array[Double])]())(_ ::: _)
      .toArray

    val originalData = rawTS.collect

    for(((ts1, data1), (ts2, data2)) <- transformedData.zip(originalData)){
      ts1 should be (ts2)
      data1.deep should be (data2.map(_ * 2.0).deep)
    }

  }

  it should "conserve the sum of its elements when partitioning" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val rawTS = IndividualRecords.generateWhiteNoise(nColumns, nSamples.toInt, deltaTMillis,
      Uniform(-0.5, 0.5),
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val partitionedSum = overlappingRDD
      .mapValues(v => v.map({case(k, v_) => v_.sum}).reduce({case ((k1, v1), (k2, v2)) => (k1, v1 + v2)}))
      .map(_._2._2)
      .reduce(_ + _)

    val directSum = rawTS.map(_._2.sum).reduce(_ + _)

    partitionedSum should be (directSum +- 0.000001)

  }

  it should " return the sum of samples when data is filled with 1.0 value" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val rawTS = IndividualRecords.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val partitionedSum = overlappingRDD
      .mapValues(v => v.map({case(k, v_) => v_.sum}).reduce({case ((k1, v1), (k2, v2)) => (k1, v1 + v2)}))
      .map(_._2._2)
      .reduce(_ + _)

    partitionedSum should be (nSamples.toDouble * nColumns.toDouble +- 0.000001)

  }

  it should "conserve the sum of its elements when partitioning when deltaT != 1" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 3L
    val nPartitions   = 8

    val rawTS = IndividualRecords.generateWhiteNoise(nColumns, nSamples.toInt, deltaTMillis,
      Uniform(-0.5, 0.5),
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val partitionedSum = overlappingRDD
      .mapValues(v => v.map({case(k, v_) => v_.sum}).reduce({case ((k1, v1), (k2, v2)) => (k1, v1 + v2)}))
      .map(_._2._2)
      .reduce(_ + _)

    val directSum = rawTS.map(_._2.sum).reduce(_ + _)

    partitionedSum should be (directSum +- 0.000001)

  }

  it should " return the sum of samples when data is filled with 1.0 value and deltaT != 1" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 4L
    val nPartitions   = 8

    val rawTS = IndividualRecords.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val partitionedSum = overlappingRDD
      .mapValues(v => v.map({case(k, v_) => v_.sum}).reduce({case ((k1, v1), (k2, v2)) => (k1, v1 + v2)}))
      .map(_._2._2)
      .reduce(_ + _)

    partitionedSum should be (nSamples.toDouble * nColumns.toDouble +- 0.000001)

  }

  it should "return a proper overlap aware iterator" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 4L
    val nPartitions   = 8

    val rawTS = IndividualRecords.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val nonOverlappingSeqs = overlappingRDD
      .mapValues(_.toArray)

    val transformedData = nonOverlappingSeqs
      .collect()
      .map(_._2.toList)
      .foldRight(List[(TSInstant, Array[Double])]())(_ ::: _)
      .toArray

    val originalData = rawTS.collect

    for(((ts1, data1), (ts2, data2)) <- transformedData.zip(originalData)){
      ts1 should be (ts2)
      data1.deep should be (data2.deep)
    }

  }

  it should " properly reduce elements" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 4L
    val nPartitions   = 8

    val rawTS = IndividualRecords.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val reduceFromOverlappingData = overlappingRDD
      .mapValues(v => v.reduce({case ((k1, v1), (k2, v2)) => (k1, v1.zip(v2).map({case(x, y) => x + y}))}))
      .map(_._2)
      .reduce({case ((k1, v1), (k2, v2)) => (k1, v1.zip(v2).map({case(x, y) => x + y}))})

    val reduceFromNonOverlappingData = rawTS
      .reduce({case ((k1, v1), (k2, v2)) => (k1, v1.zip(v2).map({case(x, y) => x + y}))})

    reduceFromOverlappingData._2.deep should be (reduceFromNonOverlappingData._2.deep)

  }

  it should " properly fold elements" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 4L
    val nPartitions   = 8

    val zeroArray   = Array.fill(nColumns)(0.0)
    val zeroSecond  = TSInstant(new DateTime(0L))

    val rawTS = IndividualRecords.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val foldFromOverlappingData = overlappingRDD
      .mapValues(v => v.fold((zeroSecond, zeroArray))(
        {case (k1, v1) => (k1, v1)},
        {case ((k1, v1), (k2, v2)) => (k1, v1.zip(v2).map({case(x, y) => x + y}))}))
      .map(_._2)
      .reduce({case ((k1, v1), (k2, v2)) => (k1, v1.zip(v2).map({case(x, y) => x + y}))})

    val foldFromNonOverlappingData = rawTS
      .fold((zeroSecond, zeroArray))(
        {case ((k1, v1), (k2, v2)) => (k1, v1.zip(v2).map({case(x, y) => x + y}))}
    )

    foldFromOverlappingData._2.deep should be (foldFromNonOverlappingData._2.deep)

  }

  it should " properly slide fold elements" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val zeroArray   = Array.fill(nColumns)(0.0)
    val zeroSecond  = TSInstant(new DateTime(0L))

    val rawTS = IndividualRecords.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    def sumArray(data: Array[(TSInstant, Array[Double])]): Double = {
      data.map(_._2.sum).sum
    }

    val foldFromOverlappingData = overlappingRDD
      .mapValues(v => v.slidingFold(Array(IntervalSize(1.0, 1.0)))(sumArray, 0.0, (x: Double, y: Double) => x + y))
      .map(_._2)
      .reduce(_ + _)

    foldFromOverlappingData should be ((3 * (nSamples - 2) + 2) * nColumns)

  }

  it should " properly slide fold elements with external targets" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val zeroArray   = Array.fill(nColumns)(0.0)
    val zeroSecond  = TSInstant(new DateTime(0L))

    val rawTS = IndividualRecords.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    def sumArray(data: Array[(TSInstant, Array[Double])]): Double = {
      data.map(_._2.sum).sum
    }

    val foldFromOverlappingData = overlappingRDD
      .mapValues(v => v.slidingFold(Array(IntervalSize(1.0, 1.0)),
                                    v.locations.slice(v.firstValidIndex, v.lastValidIndex + 1))
                                    (sumArray, 0.0, (x: Double, y: Double) => x + y))
      .map(_._2)
      .reduce(_ + _)

    foldFromOverlappingData should be ((3 * (nSamples - 2) + 2) * nColumns)

  }

  it should " properly count elements" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val rawTS = IndividualRecords.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val overlappingCount = overlappingRDD
      .mapValues(_.count)
      .map(_._2)
      .reduce(_ + _)

    overlappingCount should be (nSamples)

  }

  it should " properly filter elements" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val rawTS = IndividualRecords.generateOnes(nColumns, nSamples.toInt, deltaTMillis,
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val filteredOverlappingRDDCount = overlappingRDD
      .mapValues(v => v.filter({case (k, _) => k.timestamp.getMillis % 4 == 0}).count)
      .map(_._2)
      .reduce(_ + _)

    filteredOverlappingRDDCount should be (nSamples / 4)

  }

  it should " provide a proper natural sliding operator." in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 2L
    val nPartitions   = 8

    val rawTS = IndividualRecords.generateWhiteNoise(nColumns, nSamples.toInt, deltaTMillis,
      Uniform(-0.5, 0.5),
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val flattenedRawTS = rawTS
      .collect()
      .foldRight(List[(TSInstant, Array[Double])]())(_ :: _)
      .toArray

    val slideFromRawTS = flattenedRawTS.sliding(11).toArray

    val intervalSize = IntervalSize(10, 10)

    val slidingRDD = overlappingRDD
      .mapValues(_.sliding(Array(intervalSize))(x => x).toArray)
      .map(_._2)
      .map(_.map(_._2))
      .collect()
      .map(_.toList)
      .foldRight(List[Array[(TSInstant, Array[Double])]]())(_ ::: _)
      .filter(_.length == 11)
      .toArray

    for((data1, data2) <- slideFromRawTS.zip(slidingRDD)){
      data1.length should be (data2.length)
      for(((ts1, values1), (ts2, values2)) <- data1.zip(data2)) {
        ts1.timestamp.getMillis should be(ts2.timestamp.getMillis)
        values1.deep should be(values2.deep)
      }
    }
  }

  it should " provide a proper external sliding operator." in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 20L
    val deltaTMillis  = 2L
    val nPartitions   = 8

    val rawTS = IndividualRecords.generateWhiteNoise(nColumns, nSamples.toInt, deltaTMillis,
      Uniform(-0.5, 0.5),
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val flattenedRawTS = rawTS
      .collect()
      .foldRight(List[(TSInstant, Array[Double])]())(_ :: _)
      .toArray

    val slideFromRawTS = flattenedRawTS.sliding(11).toArray

    val intervalSize = IntervalSize(10, 10)

    val slidingRDD = overlappingRDD
      .mapValues(v => v.sliding(Array(intervalSize),
                                v.locations.slice(v.firstValidIndex, v.lastValidIndex + 1))
                                (x => x)
                                .toArray)
      .map(_._2)
      .map(_.map(_._2))
      .collect()
      .map(_.toList)
      .foldRight(List[Array[(TSInstant, Array[Double])]]())(_ ::: _)
      .filter(_.length == 11)
      .toArray

    slideFromRawTS.length should be(slidingRDD.length)

    for((data1, data2) <- slideFromRawTS.zip(slidingRDD)){
      data1.length should be (data2.length)
      for(((ts1, values1), (ts2, values2)) <- data1.zip(data2)) {
        ts1.timestamp.getMillis should be(ts2.timestamp.getMillis)
        values1.deep should be(values2.deep)
      }
    }

  }

  it should " provide a proper sliding window operator." in {

    val nColumns      = 10
    val nSamples      = 80000L
    val paddingMillis = 2000L
    val deltaTMillis  = 200L
    val nPartitions   = 8

    val rawTS = IndividualRecords.generateWhiteNoise(nColumns, nSamples.toInt, deltaTMillis,
      Uniform(-0.5, 0.5),
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, Array[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val flattenedRawTS = rawTS
      .collect()
      .foldRight(List[(TSInstant, Array[Double])]())(_ :: _)
      .toArray

    val sliceFromRawTS = flattenedRawTS.sliding(5, 5).toArray

    val cutPredicate = (x: TSInstant, y: TSInstant) => x.timestamp.secondOfDay != y.timestamp.secondOfDay

    val slicedRDD = overlappingRDD
      .mapValues(v => v.slicingWindow(Array(cutPredicate))(x => x).toArray)
      .map(_._2)
      .map(_.map(_._2))
      .collect()
      .map(_.toList)
      .foldRight(List[Array[(TSInstant, Array[Double])]]())(_ ::: _)
      .toArray

    sliceFromRawTS.length should be (slicedRDD.length + 1) // We count intervals

    for((data1, data2) <- sliceFromRawTS.zip(slicedRDD)){
      data1.length should be (data2.length)
      for(((ts1, values1), (ts2, values2)) <- data1.zip(data2)) {
        ts1.timestamp.getMillis should be(ts2.timestamp.getMillis)
        values1.deep should be(values2.deep)
      }
    }

  }


}
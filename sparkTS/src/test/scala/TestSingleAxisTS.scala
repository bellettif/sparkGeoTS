package test.scala

/**
 * Created by Francois Belletti on 8/17/15.
 */

import breeze.linalg.{min, DenseVector, sum}
import breeze.numerics._
import breeze.stats.distributions.Uniform
import main.scala.overlapping.containers._
import main.scala.overlapping.analytics._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}


class TestSingleAxisTS extends FlatSpec with Matchers{

  val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
  val sc    = new SparkContext(conf)

  "A SingleAxisTS " should " properly give an array based representation with DateTime timestamps" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val padding       = new DateTime(20L)
    val deltaT        = new DateTime(4L)
    val nPartitions   = 8

    val config = new TSConfig(nSamples, deltaT, padding, padding)

    val rawTS = Surrogate.generateWhiteNoise(
      nColumns,
      nSamples.toInt,
      deltaT,
      Uniform(-0.5, 0.5),
      DenseVector.ones[Double](nColumns),
      sc)

    val (timeSeries, _) = SingleAxisTS(nPartitions, config, rawTS)

    val nonOverlappingSeqs = timeSeries.toArray()

    val originalData = rawTS.collect()

    for(((ts1, data1), (ts2, data2)) <- nonOverlappingSeqs.zip(originalData)){
      ts1 should be (ts2)
      for(j <- 0 until data1.size){
        data1(j) should be (data2(j))
      }
    }

  }

  it should " properly give an array based representation with Long timestamps" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val padding       = 20L
    val deltaT        = 4L
    val nPartitions   = 8

    val config = new TSConfig(nSamples, deltaT, padding, padding)

    val rawTS: RDD[(Long, DenseVector[Double])] = Surrogate.generateWhiteNoise(
      nColumns,
      nSamples.toInt,
      deltaT,
      Uniform(-0.5, 0.5),
      DenseVector.ones[Double](nColumns),
      sc)

    val (timeSeries, _) = SingleAxisTS(nPartitions, config, rawTS)

    val nonOverlappingSeqs = timeSeries.toArray()

    val originalData = rawTS.collect()

    for(((ts1, data1), (ts2, data2)) <- nonOverlappingSeqs.zip(originalData)){
      ts1 should be (ts2)
      for(j <- 0 until data1.size){
        data1(j) should be (data2(j))
      }
    }

  }

  it should " properly perform a sliding operation " in {

    val nColumns      = 10
    val nSamples      = 80000L
    val padding       = new DateTime(20L)
    val deltaT        = new DateTime(4L)
    val nPartitions   = 8

    val h = 5

    val config = new TSConfig(nSamples, deltaT, padding, padding)

    val rawTS = Surrogate.generateWhiteNoise(
      nColumns,
      nSamples.toInt,
      deltaT,
      Uniform(-0.5, 0.5),
      DenseVector.ones[Double](nColumns),
      sc)

    val (timeSeries, _) = SingleAxisTS(nPartitions, config, rawTS)

    val nonOverlappingSeqs = timeSeries.toArray()

    val result = timeSeries.sliding[Double](
      {case (x, y) => abs(x.getMillis - y.getMillis) <= (deltaT.getMillis * h)})(
      {input => {
        if(input.length != 2 * h + 1){
          0.0
        }else {
          sum(input.map(_._2).reduce(_ + _))
        }
      }})
      .toArray()

    for(i <- 0 until nSamples.toInt){

      result(i)._1 should be (nonOverlappingSeqs(i)._1)

      if(i < h){
        result(i)._2 should be (0.0)
      }else if(i < nSamples - h){
        result(i)._2 should be (sum(nonOverlappingSeqs.slice(i - h, i + h + 1).map(_._2).reduce(_ + _)))
      }else{
        result(i)._2 should be (0.0)
      }

    }

  }

  it should " properly perform a sliding with memory operation " in {

    val nColumns      = 10
    val nSamples      = 80000L
    val padding       = new DateTime(20L)
    val deltaT        = new DateTime(4L)
    val nPartitions   = 1 // With more than 1 partition cannot exactly predict the result

    val h = 5

    val config = new TSConfig(nSamples, deltaT, padding, padding)

    val rawTS = Surrogate.generateWhiteNoise(
      nColumns,
      nSamples.toInt,
      deltaT,
      Uniform(-0.5, 0.5),
      DenseVector.ones[Double](nColumns),
      sc)

    val (timeSeries, _) = SingleAxisTS(nPartitions, config, rawTS)

    val nonOverlappingSeqs = timeSeries.toArray()

    val result = timeSeries.slidingWithMemory[Double, Int](
    {case (x, y) => abs(x.getMillis - y.getMillis) <= (deltaT.getMillis * h)})(
    {case (input, memState) => {
      if(input.length != 2 * h + 1){
        (0.0, memState + 1)
      }else {
        (sum(input.map(_._2).reduce(_ + _)) + memState.toDouble, memState + 1)
      }
    }},
    0)
      .toArray()

    for(i <- 0 until nSamples.toInt){

      result(i)._1 should be (nonOverlappingSeqs(i)._1)

      if(i < h){
        result(i)._2 should be (0.0)
      }else if(i < nSamples - h){
        result(i)._2 should be (sum(nonOverlappingSeqs.slice(i - h, i + h + 1).map(_._2).reduce(_ + _)) + i)
      }else{
        result(i)._2 should be (0.0)
      }

    }

  }

  it should " properly perform a filter operation" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val padding       = new DateTime(20L)
    val deltaT        = new DateTime(4L)
    val nPartitions   = 8 // With more than 1 partition cannot exactly predict the result

    val config = new TSConfig(nSamples, deltaT, padding, padding)

    val rawTS = Surrogate.generateWhiteNoise(
      nColumns,
      nSamples.toInt,
      deltaT,
      Uniform(-0.5, 0.5),
      DenseVector.ones[Double](nColumns),
      sc)

    val (timeSeries, _) = SingleAxisTS(nPartitions, config, rawTS)

    val nonOverlappingSeqs = timeSeries.toArray().filter(x => x._2(3) <= 0.0)

    val result = timeSeries.filter({case (x, y) => y(3) <= 0.0}).toArray()

    for((x, y) <- nonOverlappingSeqs.zip(result)){
      x should be (y)
    }

  }

  it should " properly perform a map operation" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val padding       = new DateTime(20L)
    val deltaT        = new DateTime(4L)
    val nPartitions   = 8 // With more than 1 partition cannot exactly predict the result

    val config = new TSConfig(nSamples, deltaT, padding, padding)

    val rawTS = Surrogate.generateWhiteNoise(
      nColumns,
      nSamples.toInt,
      deltaT,
      Uniform(-0.5, 0.5),
      DenseVector.ones[Double](nColumns),
      sc)

    val (timeSeries, _) = SingleAxisTS(nPartitions, config, rawTS)

    val nonOverlappingSeqs = timeSeries.toArray().map(x => (x._1, x._2 :* 2.0))

    val result = timeSeries.map({case (x, y) => y :* 2.0}).toArray()

    for((x, y) <- nonOverlappingSeqs.zip(result)){
      x should be (y)
    }

  }

  it should " properly perform a reduce operation" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val padding       = 20L
    val deltaT        = 4L
    val nPartitions   = 8 // With more than 1 partition cannot exactly predict the result

    val config = new TSConfig(nSamples, deltaT, padding, padding)

    val rawTS = Surrogate.generateWhiteNoise(
      nColumns,
      nSamples.toInt,
      deltaT,
      Uniform(-0.5, 0.5),
      DenseVector.ones[Double](nColumns),
      sc)

    val (timeSeries, _) = SingleAxisTS(nPartitions, config, rawTS)

    val result_1 = timeSeries.toArray().reduce[(Long, DenseVector[Double])](
      {case (t_x_1, t_x_2) => (min(t_x_1._1, t_x_2._1), t_x_1._2 + t_x_2._2)}
    )

    val result_2 = timeSeries.reduce(
      {case ((t_1, x_1), (t_2, x_2)) => (min(t_1, t_2), x_1 + x_2)}
    )

    result_1._1 should be (result_2._1)

    for(j <- 0 until nColumns) {
      result_1._2(j) should be(result_2._2(j) +- 1e-6)
    }

  }

  it should " properly perform a fold operation" in {

    val nColumns      = 10
    val nSamples      = 80000L
    val padding       = 20L
    val deltaT        = 4L
    val nPartitions   = 8 // With more than 1 partition cannot exactly predict the result

    val config = new TSConfig(nSamples, deltaT, padding, padding)

    val rawTS = Surrogate.generateWhiteNoise(
      nColumns,
      nSamples.toInt,
      deltaT,
      Uniform(-0.5, 0.5),
      DenseVector.ones[Double](nColumns),
      sc)

    val (timeSeries, _) = SingleAxisTS(nPartitions, config, rawTS)

    val zero = (-1L, DenseVector.zeros[Double](nColumns))

    val result_1 = timeSeries.toArray().fold[(Long, DenseVector[Double])](zero)(
    {case (t_x_1, t_x_2) => (min(t_x_1._1, t_x_2._1), t_x_1._2 + t_x_2._2)}
    )

    val result_2 = timeSeries.fold(zero)(
    {case (x, y) => (x, y)},
    {case ((t_1, x_1), (t_2, x_2)) => (min(t_1, t_2), x_1 + x_2)}
    )

    result_1._1 should be (result_2._1)

    for(j <- 0 until nColumns) {
      result_1._2(j) should be(result_2._2(j) +- 1e-6)
    }

  }

  it should " properly perform a sliding fold operation with memory " in {

    val nColumns      = 10
    val nSamples      = 80000L
    val padding       = new DateTime(20L)
    val deltaT        = new DateTime(4L)
    val nPartitions   = 1

    val h = 5

    val config = new TSConfig(nSamples, deltaT, padding, padding)

    val rawTS = Surrogate.generateOnes(
      nColumns,
      nSamples.toInt,
      deltaT,
      sc)

    val (timeSeries, _) = SingleAxisTS(nPartitions, config, rawTS)

    val result = timeSeries.slidingFoldWithMemory[Double, Long](
    {case (x, y) => abs(x.getMillis - y.getMillis) <= (deltaT.getMillis * h)})(
    {case (input, memState) => {
      if(input.length != 2 * h + 1){
        (memState, memState + 1L)
      }else {
        (sum(input.map(_._2).reduce(_ + _)) + memState, memState + 1L)
      }
    }},
    0.0,
    _ + _,
    0L)

    result should be ((((2 * h + 1) * (nSamples - 2 * h)) * nColumns).toDouble + nSamples * (nSamples - 1) * 0.5)

  }

  it should " properly count its elements " in {

    val nColumns      = 10
    val nSamples      = 80000L
    val padding       = new DateTime(20L)
    val deltaT        = new DateTime(4L)
    val nPartitions   = 8

    val h = 5

    val config = new TSConfig(nSamples, deltaT, padding, padding)

    val rawTS = Surrogate.generateOnes(
      nColumns,
      nSamples.toInt,
      deltaT,
      sc)

    val (timeSeries, _) = SingleAxisTS(nPartitions, config, rawTS)

    val result = timeSeries.count

    result should be (nSamples)

  }


}
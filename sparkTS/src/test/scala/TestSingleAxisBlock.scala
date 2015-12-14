package test.scala

/**
 * Created by Francois Belletti on 8/17/15.
 */

import breeze.linalg.{sum, DenseVector}
import breeze.numerics.abs
import breeze.stats.distributions.Uniform
import main.scala.overlapping.analytics._
import main.scala.overlapping.containers._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}


class TestSingleAxisBlock extends FlatSpec with Matchers{

  "A SingleAxisBlock" should " properly initialize its data member" in {

    val nColumns      = 10
    val nSamples      = 8000
    val deltaT        = new DateTime(4L)

    val rawData = (0 until nSamples)
      .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)),
      DenseVector.ones[Double](nColumns)))
      .toArray

    val singleAxisBlock = SingleAxisBlock(rawData)

    for((((_, _, ts1), data1), (ts2, data2)) <- rawData.zip(singleAxisBlock.data)){
      ts1 should be (ts2)
      for(j <- 0 until data1.size){
        data1(j) should be (data2(j))
      }
    }

  }

  it should " properly initialize its locations member" in {

    val nColumns      = 10
    val nSamples      = 8000
    val deltaT        = new DateTime(4L)

    val rawData = (0 until nSamples)
      .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)),
      DenseVector.ones[Double](nColumns)))
      .toArray

    val singleAxisBlock = SingleAxisBlock(rawData)

    for((((_, _, ts1), _), CompleteLocation(pidx, oidx, ts2)) <- rawData.zip(singleAxisBlock.locations)){
      ts1 should be (ts2)
      pidx should be (0)
      oidx should be (0)
    }

  }

  it should " properly give an array based representation" in {

    val nColumns      = 10
    val nSamples      = 8000
    val deltaT        = new DateTime(4L)

    val rawData = (0 until nSamples)
      .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)),
      DenseVector.ones[Double](nColumns)))
      .toArray

    val singleAxisBlock = SingleAxisBlock(rawData)

    for((((_, _, ts1), data1), (ts2, data2)) <- rawData.zip(singleAxisBlock.toArray)){
      ts1 should be (ts2)
      for(j <- 0 until data1.size){
        data1(j) should be (data2(j))
      }
    }

    }

    it should " properly find the window indices of a given timestamp" in {

      val nColumns      = 10
      val nSamples      = 8000
      val deltaT        = new DateTime(4L)

      val rawData = (0 until nSamples)
        .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)),
        DenseVector.ones[Double](nColumns)))
        .toArray

      val singleAxisBlock = SingleAxisBlock(rawData)

      val targetIndex = 19
      val beginIndex = 9
      val endIndex = 29

      val targetTimeStamp = new DateTime(deltaT.getMillis * targetIndex)

      def selection(x: DateTime, y: DateTime): Boolean = {
        abs(x.getMillis - y.getMillis) <= (deltaT.getMillis * 5)
      }


      val (startIndex, stopIndex) = singleAxisBlock.getWindowIndex(
        beginIndex,
        endIndex,
        targetTimeStamp,
        selection)

      singleAxisBlock.data(startIndex)._1.getMillis should be (targetTimeStamp.getMillis - 5 * deltaT.getMillis)
      singleAxisBlock.data(stopIndex)._1.getMillis should be (targetTimeStamp.getMillis + 5 * deltaT.getMillis)

    }

    it should " properly apply a kernel" in {

      val nColumns      = 10
      val nSamples      = 8000
      val deltaT        = new DateTime(4L)

      val rawData = (0 until nSamples)
        .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)),
        DenseVector.ones[Double](nColumns)))
        .toArray

      val singleAxisBlock = SingleAxisBlock(rawData)

      val h = 5
      val targetIndex = 19
      val beginIndex = targetIndex - h
      val endIndex = targetIndex + h

      val targetTimeStamp = new DateTime(deltaT.getMillis * targetIndex)

      def selection(x: DateTime, y: DateTime): Boolean = {
        abs(x.getMillis - y.getMillis) <= (deltaT.getMillis * h)
      }

      def kernel(input: Array[(DateTime, DenseVector[Double])]) : Double = {
        sum(input.map(_._2).reduce(_ + _))
      }

      val result = singleAxisBlock.applyKernel(targetTimeStamp, beginIndex, endIndex, kernel)

      result.get should be ((nColumns * (2 * h + 1)).toDouble)

    }

    it should " propery perform a sliding fold" in {

      val nColumns      = 10
      val nSamples      = 8000
      val deltaT        = new DateTime(4L)

      val rawData = (0 until nSamples)
        .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)),
        DenseVector.ones[Double](nColumns)))
        .toArray

      val singleAxisBlock = SingleAxisBlock(rawData)

      val h = 5
      val targetIndex = 19
      val beginIndex = targetIndex - h
      val endIndex = targetIndex + h

      val targetTimeStamp = new DateTime(deltaT.getMillis * targetIndex)

      def selection(x: DateTime, y: DateTime): Boolean = {
        abs(x.getMillis - y.getMillis) <= (deltaT.getMillis * h)
      }

      def kernel(input: Array[(DateTime, DenseVector[Double])]) : Double = {

        if(input.length != 2 * h + 1){
          return 0.0
        }

        sum(input.map(_._2).reduce(_ + _))
      }

      def op(x: Double, y: Double): Double = x + y

      val result = singleAxisBlock.slidingFold(selection)(kernel, 0.0, op)

      result should be (nColumns * (2 * h + 1) * (nSamples - 2 * h).toDouble)


    }

}
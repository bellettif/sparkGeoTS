package test.scala

/**
 * Created by Francois Belletti on 8/17/15.
 */

import breeze.linalg.{DenseVector, sum}
import breeze.numerics.{sqrt, abs}
import main.scala.overlapping.containers._
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

  it should " properly initialize first valid index and last valid index" in {

    val nColumns      = 10
    val nSamples      = 8000
    val deltaT        = new DateTime(4L)

    val rawData = (0 until nSamples)
      .map(x => ((x % 2, 1, implicitly[TSInstant[DateTime]].times(deltaT, x)),
      DenseVector.ones[Double](nColumns)))
      .toArray

    val singleAxisBlock = SingleAxisBlock(rawData)

    singleAxisBlock.firstValidIndex should be (1)
    singleAxisBlock.lastValidIndex should be (nSamples - 1)

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

    def kernel(input: Array[(DateTime, DenseVector[Double])]) : Double = {
      sum(input.map(_._2).reduce(_ + _))
    }

    val result = singleAxisBlock.applyKernel(targetTimeStamp, beginIndex, endIndex, kernel)

    result.get should be ((nColumns * (2 * h + 1)).toDouble)

  }

  it should " properly apply a kernel with memory" in {

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

    def kernel(input: Array[(DateTime, DenseVector[Double])], state: Double) : (Double, Double) = {
      (sum(input.map(_._2).reduce(_ + _)), state + .1478)
    }

    val memState = 0.0

    val result = singleAxisBlock.applyMemoryKernel(targetTimeStamp, beginIndex, endIndex, memState, kernel)

    (result._1.get, result._2) should be ((nColumns * (2 * h + 1)).toDouble, .1478)

  }


  it should " propery perform a sliding " in {

    val nColumns      = 10
    val nSamples      = 8000
    val deltaT        = new DateTime(4L)

    val rawData = (0 until nSamples)
      .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)),
      DenseVector.ones[Double](nColumns)))
      .toArray

    val singleAxisBlock = SingleAxisBlock(rawData)

    val h = 5

    def selection(x: DateTime, y: DateTime): Boolean = {
      abs(x.getMillis - y.getMillis) <= (deltaT.getMillis * h)
    }

    def kernel(input: Array[(DateTime, DenseVector[Double])]) : Double = {

      if(input.length != 2 * h + 1){
        return 0.0
      }

      sum(input.map(_._2).reduce(_ + _))
    }


    val result = singleAxisBlock.sliding(selection)(kernel)
    for(i <- 0 until nSamples){

      result.locations(i) should be (CompleteLocation(rawData(i)._1._1, rawData(i)._1._2, rawData(i)._1._3))

      if(i < h){
        result.data(i) should be (rawData(i)._1._3, 0.0)
      }else if(i < nSamples - h){
        result.data(i) should be (rawData(i)._1._3, nColumns * (2 * h + 1))
      }else{
        result.data(i) should be (rawData(i)._1._3, 0.0)
      }

    }

  }

  it should " propery perform a sliding with memory" in {

    val nColumns      = 10
    val nSamples      = 8000
    val deltaT        = new DateTime(4L)

    val rawData = (0 until nSamples)
      .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)),
      DenseVector.ones[Double](nColumns)))
      .toArray

    val singleAxisBlock = SingleAxisBlock(rawData)

    val h = 5

    def selection(x: DateTime, y: DateTime): Boolean = {
      abs(x.getMillis - y.getMillis) <= (deltaT.getMillis * h)
    }

    def kernel(input: Array[(DateTime, DenseVector[Double])], memState: Double) : (Double, Double) = {

      if(input.length != 2 * h + 1){
        return (0.0 + memState, memState + 1)
      }

      (sum(input.map(_._2).reduce(_ + _)) + memState, memState + 1)
    }

    val result = singleAxisBlock.slidingWithMemory(selection)(kernel, 0.0)

    for(i <- 0 until nSamples){

      result.locations(i) should be (CompleteLocation(rawData(i)._1._1, rawData(i)._1._2, rawData(i)._1._3))

      if(i < h){
        result.data(i) should be (rawData(i)._1._3, i.toDouble)
      }else if(i < nSamples - h){
        result.data(i) should be (rawData(i)._1._3, nColumns * (2 * h + 1) + i.toDouble)
      }else{
        result.data(i) should be (rawData(i)._1._3, i.toDouble)
      }

    }

  }

  it should " properly filter out data" in {

    val nColumns      = 10
    val nSamples      = 8000
    val deltaT        = new DateTime(4L)

    val rawData = (0 until nSamples)
      .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)), x))
      .toArray

    val singleAxisBlock = SingleAxisBlock(rawData)

    def predicate(timestamp: DateTime, observation: Int): Boolean ={
      (observation % 2) == 0
    }

    val result = singleAxisBlock.filter(predicate)

    result.data.length should be (nSamples / 2)
    result.locations.length should be (nSamples / 2)

    for(i <- 0 until nSamples / 2){
      result.data(i) should be (rawData(2 * i)._1._3, rawData(2 * i)._2)
      result.locations(i) should be (CompleteLocation(
        rawData(2 * i)._1._1,
        rawData(2 * i)._1._2,
        rawData(2 * i)._1._3))
    }

  }

  it should " properly map a function" in {

    val nColumns      = 10
    val nSamples      = 8000
    val deltaT        = new DateTime(4L)

    val rawData = (0 until nSamples)
      .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)), x))
      .toArray

    val singleAxisBlock = SingleAxisBlock(rawData)

    def myFunction(timestamp: DateTime, observation: Int): Double ={
      sqrt(observation.toDouble)
    }

    val result = singleAxisBlock.map(myFunction)

    result.data.length should be (nSamples)
    result.locations.length should be (nSamples)

    for(i <- 0 until nSamples){
      result.data(i) should be (rawData(i)._1._3, sqrt(rawData(i)._2.toDouble))
      result.locations(i) should be (CompleteLocation(
        rawData(i)._1._1,
        rawData(i)._1._2,
        rawData(i)._1._3))
    }

  }

  it should " properly perform a reduction" in {

    val nColumns      = 10
    val nSamples      = 8000
    val deltaT        = new DateTime(4L)

    val rawData = (0 until nSamples)
      .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)), 1))
      .toArray

    val singleAxisBlock = SingleAxisBlock(rawData)

    def op(t1_x1: (DateTime, Int), t2_x2: (DateTime, Int)): (DateTime, Int) ={
      (t1_x1._1, t1_x1._2 + t2_x2._2)
    }

    val result = singleAxisBlock.reduce(op)

    result._1 should be (rawData.head._1._3)

    result._2 should be (nSamples)

  }

  it should " properly perform a fold" in {

    val nColumns      = 10
    val nSamples      = 8000
    val deltaT        = new DateTime(4L)

    val rawData = (0 until nSamples)
      .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)), 1))
      .toArray

    val singleAxisBlock = SingleAxisBlock(rawData)

    def myFunction(timestamp: DateTime, observation: Int): Double ={
      sqrt(observation.toDouble)
    }

    def op(x: Double, y: Double): Double ={
      x + y
    }

    val result = singleAxisBlock.fold(1.25097)(myFunction, op)

    result should be (rawData.map(x => sqrt(x._2.toDouble)).reduce(_ + _) + 1.25097)

  }

  it should " properly perform a sliding fold" in {

    val nColumns      = 10
    val nSamples      = 8000
    val deltaT        = new DateTime(4L)

    val rawData = (0 until nSamples)
      .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)),
      DenseVector.ones[Double](nColumns)))
      .toArray

    val singleAxisBlock = SingleAxisBlock(rawData)

    val h = 5

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

  it should " properly perform a sliding fold with memory" in {

    val nColumns      = 10
    val nSamples      = 8000
    val deltaT        = new DateTime(4L)

    val rawData = (0 until nSamples)
      .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)),
      DenseVector.ones[Double](nColumns)))
      .toArray

    val singleAxisBlock = SingleAxisBlock(rawData)

    val h = 5

    def selection(x: DateTime, y: DateTime): Boolean = {
      abs(x.getMillis - y.getMillis) <= (deltaT.getMillis * h)
    }

    def kernel(input: Array[(DateTime, DenseVector[Double])], memState : Double): (Double, Double) = {

      if(input.length != 2 * h + 1){
        return (memState, memState + 1.0)
      }

      (sum(input.map(_._2).reduce(_ + _)) + memState, memState + 1.0)

    }

    def op(x: Double, y: Double): Double = x + y

    val result = singleAxisBlock.slidingFoldWithMemory(selection)(kernel, 0.0, op, 0.0)

    result should be (nColumns * (2 * h + 1) * (nSamples - 2 * h).toDouble + (nSamples * (nSamples - 1)).toDouble * 0.5)

  }

  it should " properly convert itself to an array" in {

    val nColumns      = 10
    val nSamples      = 8000
    val deltaT        = new DateTime(4L)

    val rawData = (0 until nSamples)
      .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)),
      DenseVector.ones[Double](nColumns)))
      .toArray

    val singleAxisBlock = SingleAxisBlock(rawData)

    val result = singleAxisBlock.toArray

    for(i <- 0 until nSamples){
      result(i)._1 should be (rawData(i)._1._3)
      result(i)._2 should be (rawData(i)._2)
    }

  }

  it should " properly count the number of its elements" in {

    val nColumns      = 10
    val nSamples      = 8000
    val deltaT        = new DateTime(4L)

    val rawData = (0 until nSamples)
      .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)),
      DenseVector.ones[Double](nColumns)))
      .toArray

    val singleAxisBlock = SingleAxisBlock(rawData)

    val result = singleAxisBlock.count

    result should be (nSamples)

  }

  it should " properly perform a take operation" in {

    val nColumns      = 10
    val nSamples      = 8000
    val deltaT        = new DateTime(4L)

    val rawData = (0 until nSamples)
      .map(x => ((0, 0, implicitly[TSInstant[DateTime]].times(deltaT, x)),
      DenseVector.ones[Double](nColumns)))
      .toArray

    val singleAxisBlock = SingleAxisBlock(rawData)

    val toTake = 100

    val result = singleAxisBlock.take(toTake)

    for(i <- 0 until toTake){
      result(i)._1 should be (rawData(i)._1._3)
      result(i)._2 should be (rawData(i)._2)
    }

  }

}
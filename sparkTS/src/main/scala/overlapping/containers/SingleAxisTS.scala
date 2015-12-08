package main.scala.overlapping.containers

import breeze.numerics.sqrt
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object SingleAxisTS{

  def apply[IndexT : TSInstant : ClassTag, ValueT : ClassTag](
      nPartitions: Int,
      config: SingleAxisConfig[IndexT],
      rawData: RDD[(IndexT, ValueT)]): (SingleAxisTS[IndexT, ValueT], Array[(IndexT, IndexT)]) = {

    val nSamples = rawData.count()

    val intervals = IntervalSampler(
        nPartitions,
        sqrt(nSamples).toInt,
        rawData,
        Some(nSamples))

    val replicator = new SingleAxisReplicator[IndexT, ValueT](intervals, config.selection)
    val partitioner = new BlockIndexPartitioner(intervals.length)

    val content = rawData
      .flatMap({ case (k, v) => replicator.replicate(k, v) })

    val content_ = content
      .partitionBy(partitioner)
      .mapPartitionsWithIndex({case (i, x) => ((i, SingleAxisBlock(x.toArray)) :: Nil).toIterator}, true)

    (new SingleAxisTS(config, content_), intervals)

  }

}


/**
 * Created by Francois Belletti on 12/8/15.
 */
class SingleAxisTS[IndexT : TSInstant : ClassTag, ValueT : ClassTag](
      val config: SingleAxisConfig[IndexT],
      val content: RDD[(Int, SingleAxisBlock[IndexT, ValueT])]) extends KernelizedTS[IndexT, ValueT]{

  def sliding[ResultT: ClassTag](
      selection: (IndexT, IndexT) => Boolean,
      kernel: Array[(IndexT, ValueT)] => ResultT,
      targetFilter: Option[IndexT => Boolean] = None,
      windowFilter: Option[Array[(IndexT, ValueT)] => Boolean] = None): SingleAxisTS[IndexT, ResultT] = {

    new SingleAxisTS(
      config,
      content.mapValues(_.sliding(selection)(kernel, targetFilter, windowFilter)))

  }

  def slidingWithMemory[ResultT: ClassTag, MemType: ClassTag](
      selection: (IndexT, IndexT) => Boolean,
      kernel: (Array[(IndexT, ValueT)], MemType) => (ResultT, MemType),
      init: MemType,
      targetFilter: Option[IndexT => Boolean] = None,
      windowFilter: Option[Array[(IndexT, ValueT)] => Boolean] = None): SingleAxisTS[IndexT, ResultT] = {

    new SingleAxisTS(
      config,
      content.mapValues(_.slidingWithMemory(selection)(kernel, init, targetFilter, windowFilter))
    )

  }

  def filter(p: (IndexT, ValueT) => Boolean): KernelizedTS[IndexT, ValueT] = {

    new SingleAxisTS(
      config,
      content.mapValues(_.filter(p))
    )

  }

  def map[ResultT: ClassTag](
      f: (IndexT, ValueT) => ResultT,
      filter: Option[(IndexT, ValueT) => Boolean] = None): SingleAxisTS[IndexT, ResultT] = {

    new SingleAxisTS(
      config,
      content.mapValues(_.map(f, filter))
    )

  }

  def reduce(
    f: ((IndexT, ValueT), (IndexT, ValueT)) => (IndexT, ValueT),
    filter: Option[(IndexT, ValueT) => Boolean] = None): (IndexT, ValueT) = {

    content
      .mapValues(_.reduce(f, filter))
      .map(_._2).reduce(f)

  }

  def fold[ResultT: ClassTag](zeroValue: ResultT)(
      f: (IndexT, ValueT) => ResultT,
      op: (ResultT, ResultT) => ResultT,
      filter: Option[(IndexT, ValueT) => Boolean] = None): ResultT = {

    content
      .mapValues(_.fold(zeroValue)(f, op, filter))
      .map(_._2)
      .fold(zeroValue)(op)

  }

  def slidingFold[ResultT: ClassTag](
      selection: (IndexT, IndexT) => Boolean,
      kernel: Array[(IndexT, ValueT)] => ResultT,
      zero: ResultT,
      op: (ResultT, ResultT) => ResultT,
      targetFilter: Option[IndexT => Boolean] = None,
      windowFilter: Option[Array[(IndexT, ValueT)] => Boolean] = None): ResultT = {

    content
      .mapValues(_.slidingFold(selection)(kernel, zero, op, targetFilter, windowFilter))
      .map(_._2)
      .fold(zero)(op)

  }

  def slidingFoldWithMemory[ResultT: ClassTag, MemType: ClassTag](
      selection: (IndexT, IndexT) => Boolean,
      kernel: (Array[(IndexT, ValueT)], MemType) => (ResultT, MemType),
      zero: ResultT,
      op: (ResultT, ResultT) => ResultT,
      init: MemType,
      targetFilter: Option[IndexT => Boolean] = None,
      windowFilter: Option[Array[(IndexT, ValueT)] => Boolean] = None): ResultT = {

    content
      .mapValues(_.slidingFoldWithMemory(selection)(kernel, zero, op, init, targetFilter, windowFilter))
      .map(_._2)
      .fold(zero)(op)

  }

  def count: Long = {

    content
      .mapValues(_.count)
      .map(_._2)
      .reduce(_ + _)

  }

  def toArray(sample: Option[Int] = None): Array[(IndexT, ValueT)] = {

    if(sample.isEmpty) {

      val chunks: Array[(Int, Array[(IndexT, ValueT)])] = content
        .mapValues(_.toArray)
        .collect

      chunks
        .sortBy(_._1) // Sort by partition index
        .map(_._2)    // Extract content array
        .reduce[Array[(IndexT, ValueT)]]({case (x: Array[(IndexT, ValueT)], y: Array[(IndexT, ValueT)]) => x ++ y})

    }else{

      content
        .mapValues(_.toArray)
        .flatMap({case (k: Int, v: Array[(IndexT, ValueT)]) => v.toIterator})
        .sample(false, sample.get.toDouble / count.toDouble)
        .sortBy(_._1)
        .collect      // Sort by partition index

    }

  }

}

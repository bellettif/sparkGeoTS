package main.scala.overlapping.containers

import breeze.linalg.DenseVector
import breeze.numerics._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.reflect.ClassTag

object SingleAxisVectTS{

  def apply[IndexT : TSInstant : ClassTag](
      nPartitions: Int,
      config: SingleAxisVectTSConfig[IndexT],
      rawData: RDD[(IndexT, DenseVector[Double])]): (SingleAxisVectTS[IndexT], Array[(IndexT, IndexT)]) = {

    val nSamples = rawData.count()

    val intervals = IntervalSampler(
      nPartitions,
      sqrt(nSamples).toInt,
      rawData,
      Some(nSamples))

    val replicator = new SingleAxisReplicator[IndexT, DenseVector[Double]](intervals, config.selection)

    val partitioner = new BlockIndexPartitioner(intervals.length)

    val content = rawData
      .flatMap({ case (k, v) => replicator.replicate(k, v) })

    val content_ = content
      .partitionBy(partitioner)
      .mapPartitionsWithIndex({case (i, x) => ((i, SingleAxisBlock(x.toArray)) :: Nil).toIterator}, true)

    (new SingleAxisVectTS(config, content_), intervals)

  }

}

/**
 * Created by Francois Belletti on 12/4/15.
 */
class SingleAxisVectTS[IndexT : TSInstant : ClassTag](
    override val config: SingleAxisVectTSConfig[IndexT],
    override val content: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])])
  extends SingleAxisTS[IndexT, DenseVector[Double]](config, content)
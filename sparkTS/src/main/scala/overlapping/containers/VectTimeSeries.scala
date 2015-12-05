package main.scala.overlapping.containers

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD

/**
 * Created by Francois Belletti on 12/4/15.
 */
class VectTimeSeries[IndexT <: TSConfig[IndexT]](
    override val content: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
    override val config: VectTSConfig[IndexT]) extends TimeSeries[IndexT, DenseVector[Double]](content, config)
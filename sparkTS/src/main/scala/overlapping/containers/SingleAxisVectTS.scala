package main.scala.overlapping.containers

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 12/4/15.
 */
class SingleAxisVectTS[IndexT : TSInstant : ClassTag](
    override val config: SingleAxisVectTSConfig[IndexT],
    override val content: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])])
  extends SingleAxisTS[IndexT, DenseVector[Double]](config, content)
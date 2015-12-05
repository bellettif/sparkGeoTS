package main.scala.overlapping.containers

import org.apache.spark.rdd.RDD

/**
 * Created by Francois Belletti on 12/4/15.
 */
class TimeSeries[IndexT <: TSInstant[IndexT], ValueT](
    val content: RDD[(Int, SingleAxisBlock[IndexT, ValueT])],
    val config: TSConfig[IndexT])


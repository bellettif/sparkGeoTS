package TsUtils

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
 * Created by Francois Belletti on 7/30/15.
 */
case class TimeSeriesConfig(memory: Broadcast[Long], // Effective memory in milliseconds
                            nCols: Broadcast[Int],
                            nSamples: Broadcast[Long],
                            partitionDuration: Broadcast[Long], // Duration represented on each partition in milliseconds
                            nPartitions: Broadcast[Int],
                            partitioner: TSPartitioner)
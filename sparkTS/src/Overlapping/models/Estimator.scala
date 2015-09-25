package overlapping.models

import org.apache.spark.rdd.RDD
import overlapping.containers.block.SingleAxisBlock

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/24/15.
 */
trait Estimator[IndexT <: Ordered[IndexT], ValueT, EstimateT]
  extends Serializable{

  def windowEstimate(window: Array[(IndexT, ValueT)]): EstimateT = ???

  def blockEstimate(block: SingleAxisBlock[IndexT, ValueT]): EstimateT = ???

  def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, ValueT])]): EstimateT = ???

}

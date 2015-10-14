package overlapping.timeSeries

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import overlapping.containers.SingleAxisBlock
import overlapping.timeSeries.secondOrder.univariate.Procedures.InnovationAlgo

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/13/15.
 */
class MAModel[IndexT <: Ordered[IndexT] : ClassTag](
    q: Int,
    mean: Option[DenseVector[Double]] = None)
    (implicit config: TSConfig, sc: SparkContext)
  extends AutoCovariances[IndexT](q, mean){

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): Array[CovSignature]= {

    super
      .estimate(timeSeries)
      .map(x => InnovationAlgo(q, x.covariation))

  }


}
package overlapping.timeSeries

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import overlapping.containers.SingleAxisBlock
import overlapping.timeSeries.secondOrder.univariate.Procedures.DurbinLevinson

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/13/15.
 */
class ARModel[IndexT <: Ordered[IndexT] : ClassTag](
    p: Int,
    mean: Option[DenseVector[Double]] = None)
    (implicit config: TSConfig, sc: SparkContext)
  extends AutoCovariances[IndexT](p, mean){

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): Array[CovSignature]= {

    super
      .estimate(timeSeries)
      .map(x => DurbinLevinson(p, x.covariation))

  }



}
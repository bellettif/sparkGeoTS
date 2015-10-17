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
object ARModel{

  def apply[IndexT <: Ordered[IndexT]: ClassTag](
      timeSeries : RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
      p: Int,
      mean: Option[DenseVector[Double]] = None)
      (implicit config: TSConfig, sc: SparkContext): Array[SecondOrderSignature] ={

    val estimator = new ARModel[IndexT](p, mean)

    estimator.estimate(timeSeries)

  }

}

class ARModel[IndexT <: Ordered[IndexT] : ClassTag](
    p: Int,
    mean: Option[DenseVector[Double]] = None)
    (implicit config: TSConfig, sc: SparkContext)
  extends AutoCovariances[IndexT](p, mean){

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): Array[SecondOrderSignature]= {

    super
      .estimate(timeSeries)
      .map(x => DurbinLevinson(p, x.covariation))

  }



}
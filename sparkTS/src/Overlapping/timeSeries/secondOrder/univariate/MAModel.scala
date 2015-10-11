package overlapping.timeSeries.secondOrder.univariate

import breeze.linalg.DenseVector
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import overlapping.containers.SingleAxisBlock
import overlapping.timeSeries.secondOrder.univariate.Procedures.InnovationAlgo

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/13/15.
 */
class MAModel[IndexT <: Ordered[IndexT] : ClassTag](
    deltaT: Double,
    q: Int,
    d: Int,
    mean: Broadcast[DenseVector[Double]]
  )
  extends AutoCovariances[IndexT](deltaT, q, d, mean){

  override def windowEstimate(slice: Array[(IndexT, DenseVector[Double])]): Array[CovSignature] = {

    super
      .windowEstimate(slice)
      .map(x => InnovationAlgo(q, x.covariation))

  }

  override def blockEstimate(block: SingleAxisBlock[IndexT, DenseVector[Double]]): Array[CovSignature] = {

    super
      .blockEstimate(block)
      .map(x => InnovationAlgo(q, x.covariation))

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): Array[CovSignature]= {

    super
      .estimate(timeSeries)
      .map(x => InnovationAlgo(q, x.covariation))

  }


}
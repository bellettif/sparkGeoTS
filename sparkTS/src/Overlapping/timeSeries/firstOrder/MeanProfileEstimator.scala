package overlapping.timeSeries.firstOrder

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import overlapping.containers.SingleAxisBlock
import overlapping.timeSeries.{Estimator, FirstOrderEssStat}

import scala.collection.mutable

/**
 * Created by Francois Belletti on 9/23/15.
 */



class MeanProfileEstimator[IndexT <: Ordered[IndexT]](
    val d: Int,
    val hashFct: IndexT => Int)
  extends FirstOrderEssStat[IndexT, DenseVector[Double], mutable.HashMap[Int, (DenseVector[Double], Long)]]
  with Estimator[IndexT, DenseVector[Double], mutable.HashMap[Int, DenseVector[Double]]]{

  override def zero = new mutable.HashMap[Int, (DenseVector[Double], Long)]()

  override def kernel(datum: (IndexT,  DenseVector[Double])): mutable.HashMap[Int, (DenseVector[Double], Long)] = {
    mutable.HashMap(hashFct(datum._1) -> (datum._2, 1L))
  }

  def merge(x: (DenseVector[Double], Long), y: (DenseVector[Double], Long)): (DenseVector[Double], Long) = {
    (x._1 + y._1, x._2 + y._2)
  }

  override def reducer(map1: mutable.HashMap[Int, (DenseVector[Double], Long)],
                       map2: mutable.HashMap[Int, (DenseVector[Double], Long)]):
  mutable.HashMap[Int, (DenseVector[Double], Long)] = {

    for(k <- map2.keySet){
        map1(k) = merge(map2(k), map1.getOrElse(k, (DenseVector.zeros[Double](d), 0L)))
    }

    map1

  }

  def normalize(map: mutable.HashMap[Int, (DenseVector[Double], Long)]): mutable.HashMap[Int, DenseVector[Double]] = {
    map.map({case (k, (v1, v2)) => (k, v1 / v2.toDouble)})
  }

  override def windowEstimate(window: Array[(IndexT, DenseVector[Double])]): mutable.HashMap[Int, DenseVector[Double]] = {
    normalize(windowStats(window))
  }

  override def blockEstimate(block: SingleAxisBlock[IndexT, DenseVector[Double]]): mutable.HashMap[Int, DenseVector[Double]] = {
    normalize(blockStats(block))
  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): mutable.HashMap[Int, DenseVector[Double]] = {
    normalize(timeSeriesStats(timeSeries))
  }

}

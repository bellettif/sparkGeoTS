package main.scala.overlapping.timeSeriesOld

import breeze.linalg.DenseVector
import main.scala.overlapping.containers._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/23/15.
 */


object MeanProfileEstimator{

  def apply[IndexT : ClassTag](
      timeSeries: VectTimeSeries[IndexT],
      hashFct: TSInstant[IndexT] => Int): mutable.HashMap[Int, DenseVector[Double]] = {

    val meanProfileEstimator = new MeanProfileEstimator[IndexT](timeSeries.config, hashFct)

    meanProfileEstimator.estimate(timeSeries)

  }

  def removeSeason[IndexT](
      rawData: RDD[(TSInstant[IndexT], DenseVector[Double])],
      hashFct: TSInstant[IndexT] => Int,
      seasonProfile: mutable.HashMap[Int, DenseVector[Double]]): RDD[(TSInstant[IndexT], DenseVector[Double])] = {

    val seasonalProfile = rawData.context.broadcast(seasonProfile)
    rawData.map({ case (k, v) => (k, v - seasonalProfile.value(hashFct(k))) })

  }

}


class MeanProfileEstimator[IndexT : ClassTag](
    config: VectTSConfig[IndexT],
    hashFct: TSInstant[IndexT] => Int)
  extends FirstOrderEssStat[IndexT, mutable.HashMap[Int, (DenseVector[Double], Long)]]
  with Estimator[IndexT, mutable.HashMap[Int, DenseVector[Double]]]{

  override def zero = new mutable.HashMap[Int, (DenseVector[Double], Long)]()

  override def kernel(t: TSInstant[IndexT], v: DenseVector[Double]): mutable.HashMap[Int, (DenseVector[Double], Long)] = {
    mutable.HashMap(hashFct(t) -> (v, 1L))
  }

  def merge(x: (DenseVector[Double], Long), y: (DenseVector[Double], Long)): (DenseVector[Double], Long) = {
    (x._1 + y._1, x._2 + y._2)
  }

  override def reducer(map1: mutable.HashMap[Int, (DenseVector[Double], Long)],
                       map2: mutable.HashMap[Int, (DenseVector[Double], Long)]):
  mutable.HashMap[Int, (DenseVector[Double], Long)] = {

    for(k <- map2.keySet){
        map1(k) = merge(map2(k), map1.getOrElse(k, (DenseVector.zeros[Double](config.dim), 0L)))
    }

    map1

  }

  def normalize(map: mutable.HashMap[Int, (DenseVector[Double], Long)]): mutable.HashMap[Int, DenseVector[Double]] = {
    map.map({case (k, (v1, v2)) => (k, v1 / v2.toDouble)})
  }

  override def estimate(timeSeries: VectTimeSeries[IndexT]): mutable.HashMap[Int, DenseVector[Double]] = {
    normalize(timeSeriesStats(timeSeries))
  }

}

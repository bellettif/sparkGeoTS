package main.scala.overlapping

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import main.scala.overlapping.containers.{TSInstant, SingleAxisBlock}
import org.apache.spark.SparkContext._

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 10/12/15.
 */

object Utils {

  /**
   * Generic quantization with post-processing operation. Buckets are determined by a hash function.
   *
   * @param in Input data.
   * @param hasher Function that designs the buckets.
   * @param reducer Reducing function to aggregate elements belonging in the same hash bucket.
   * @param postReduce Function to apply to reduced results.
   * @tparam IndexT Type of timestamps.
   * @return Another (k, v) RDD where keys are timestamps.
   */
  def resample[IndexT: ClassTag, T: ClassTag, V: ClassTag](
      in: RDD[(IndexT, V)],
      hasher: IndexT => IndexT,
      preMap: ((IndexT, V)) => (IndexT, T),
      reducer: (T, T) => T,
      postReduce: ((IndexT, T)) => (IndexT, V)): RDD[(IndexT, V)] = {

    in
      .map({case (t: IndexT, v) => preMap(hasher(t), v)})
      .reduceByKey(reducer)
      .map(postReduce)

  }

  /**
   * Generic quantization. Buckets are determined by a hash function.
   *
   * @param in Input data.
   * @param hasher Function that designs the buckets.
   * @param reducer Reducing function to aggregate elements belonging in the same hash bucket.
   * @tparam IndexT Type of timestamps.
   * @return Another (k, v) RDD where keys are timestamps.
   */
  def resample[IndexT: ClassTag, V: ClassTag](
      in: RDD[(IndexT, V)],
      hasher: IndexT => IndexT,
      reducer: (V, V) => V): RDD[(IndexT, V)] = {

    in
      .map({case (t, v) => (hasher(t), v)})
      .reduceByKey(reducer)

  }

  /**
   * Summation based quantization. Buckets are determined by a hash function.
   *
   * @param in Input data.
   * @param hasher Function that designs the buckets.
   * @tparam IndexT Type of timestamps.
   * @return Another (k, v) RDD where keys are timestamps.
   */
  def sumResample[IndexT: ClassTag](
      in: RDD[(IndexT, DenseVector[Double])],
      hasher: IndexT => IndexT): RDD[(IndexT, DenseVector[Double])] = {

    resample[IndexT, DenseVector[Double]](
      in,
      hasher,
      _ + _
    )

  }

  /**
   * Average based quantization. Buckets are determined by a hash function.
   *
   * @param in Input data.
   * @param hasher Function that designs the buckets.
   * @tparam IndexT Type of timestamps.
   * @return Another (k, v) RDD where keys are timestamps.
   */
  def meanResample[IndexT: ClassTag](
      in: RDD[(IndexT, DenseVector[Double])],
      hasher: IndexT => IndexT): RDD[(IndexT, DenseVector[Double])] = {

    resample[IndexT, (DenseVector[Double], Long), DenseVector[Double]](
      in,
      hasher,
      {case (t: IndexT, v: DenseVector[Double]) => (t, (v, 1L))},
      {case ((v1: DenseVector[Double], c1: Long), (v2: DenseVector[Double], c2: Long)) => (v1 + v2, c1 + c2)},
      {case (t: IndexT, (v: DenseVector[Double], c: Long)) => (t, v / c.toDouble)}
    )

  }


}
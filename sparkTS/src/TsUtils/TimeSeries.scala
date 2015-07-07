package TsUtils

import Utils.WindowedIterator

import scala.reflect._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.OrderedRDDFunctions
import org.apache.spark.SparkContext

import org.joda.time.{DateTime, Interval}

import scala.math._

/**
 * Created by Francois Belletti on 6/24/15.
 */
class TimeSeries[RawType: ClassTag, RecordType: ClassTag](rawRDD: RDD[RawType],
                                                          timeExtract: RawType => (DateTime, Array[RecordType]),
                                                          sc: SparkContext,
                                                          memory: Option[Int]) extends Serializable{

  /*
  Initialization phase
   */

  var effectiveLag = sc.broadcast(if (memory.isDefined) memory.get else 10)


  /*
  Convert the time index to longs
   */

  implicit val RecordTimeOrdering = new Ordering[(Long, Array[RecordType])] {
    override def compare(a: (Long, Array[RecordType]), b: (Long, Array[RecordType])) =
      a._1.compare(b._1)
  }

  val timeRDD: RDD[(Long, Array[RecordType])] = {
    val tempRDD: RDD[(Long, Array[RecordType])]  = rawRDD
      .map(timeExtract)
      .map({case (t, v) => (t.getMillis, v)})
    val partitioner: Partitioner = new RangePartitioner(100, tempRDD)
    tempRDD.repartitionAndSortWithinPartitions(partitioner)
  }


  /*
  Figure out the time partitioner
   */
  val minTS = sc.broadcast(timeRDD.min()._1)
  val maxTS = sc.broadcast(timeRDD.max()._1)
  val nTS   = sc.broadcast(maxTS.value - minTS.value)
  val nCols = sc.broadcast(10)
  val partitionLength = sc.broadcast(10000)
  val nPartitions     = sc.broadcast(floor(nTS.value / partitionLength.value).toInt + 1)

  val partitioner = new TSPartitioner(nPartitions.value)

  def stitchAndTranspose(kVPairs: Iterator[((Int, Long), Array[RecordType])]): Iterator[Array[RecordType]] ={
    kVPairs.toSeq.map(_._2).transpose.map(x => Array(x: _*)).iterator
  }

  def extractTimeIndex(kVPairs: Iterator[((Int, Long), Array[RecordType])]): Iterator[Long] ={
    kVPairs.toSeq.map(_._1._2).iterator
  }

  /*
  Gather data into overlapping tiles
   */

  val augmentedIndexRDD = timeRDD
    .flatMap({case (t, v) =>
    if (((t - minTS.value) % partitionLength.value <= effectiveLag.value) &&
      (floor((t - minTS.value) / partitionLength.value).toInt > 0))
      ((floor((t - minTS.value) / partitionLength.value).toInt, t), v)::
        ((floor((t - minTS.value) / partitionLength.value).toInt - 1, t), v)::
        Nil
    else
      ((floor((t - minTS.value) / partitionLength.value).toInt, t), v)::Nil
  })
    .partitionBy(partitioner)

  val tiles = augmentedIndexRDD
    .mapPartitions(stitchAndTranspose, true)

  val timeStamps = rawRDD
    .map(x => timeExtract(x)._1)
    .flatMap(t =>
    if (((t.getMillis - minTS.value) % partitionLength.value <= effectiveLag.value) &&
      (floor((t.getMillis - minTS.value) / partitionLength.value).toInt > 0))
      (floor((t.getMillis - minTS.value) / partitionLength.value).toInt, (t.getMillis, t))::
        (floor((t.getMillis - minTS.value) / partitionLength.value).toInt - 1, (t.getMillis, t))::
        Nil
    else
      (floor((t.getMillis - minTS.value) / partitionLength.value).toInt, (t.getMillis, t))::Nil
    )
    .partitionBy(partitioner)


  /*
  Computational utils. These will not be declared here in the end.ß
   */

  def computeCrossFold[ResultType: ClassTag](cross: (RecordType, RecordType) => ResultType,
                                             foldOp: (ResultType, ResultType) => ResultType,
                                             cLeft: Int, cRight: Int, lag: Int,
                                             zero: ResultType): ResultType ={
    /*
    @brief: Compute a cross and fold operation. If fold is + and cross * this is
    a cross correlation for example.
    @param: cross: function that multiplies two row elements together. Does not have to be comm or asso.
    @param: foldOp: how to fold results of cross operations. Has to be assoc.
    @param: cLeft: index of the first column.
    @param: cRight: index of the second column.
    @param: lag: value of lag applied to the right column. A positive lag delays the right column w.r.t
     the left one.
    @param: zero: the neutral element to be used in the fold operation.
    @retval: the resulting scalar value.
     */

    def computeCrossFoldArray(data: Array[Array[RecordType]]): ResultType ={
      val leftCol: Array[RecordType]  = data.apply(cLeft)
      val rightCol: Array[RecordType] = data.apply(cRight)
      var result: ResultType = zero
      val effectiveSize = min(leftCol.size, partitionLength.value + lag)
      for(rowIdx <- 0 until (effectiveSize - lag)){
        result = foldOp(result, cross(leftCol.apply(rowIdx + lag), rightCol.apply(rowIdx)))
      }
      result
    }

    tiles.glom().map(computeCrossFoldArray).fold(zero)(foldOp(_,_))
  }

  /*
  f takes several columns and returns a result (for example linear regression)
  slicer returns true if two timestamps do not belong to the same window
   */
  def applyBy[ResultType: ClassTag](//f: Iterator[Array[RecordType]] => ResultType,
                                     slicer: (DateTime, DateTime) => Boolean) ={

    def windowEndPoints(tsGroup: Iterator[(Int, (Long, DateTime))]): Iterator[((Int, Int), (Long, DateTime))] = {
      val (it1, it2) = tsGroup.duplicate
      (it1.zipWithIndex zip it2.drop(1))
        .filter({
        case (((pIdx1, (millis1, datet1)), idx1), (pIdx2, (millis2, datet2))) => slicer(datet1, datet2)
      })
        .map({
        case (((pIdx, (millis, datet)), idx), _) => ((pIdx, idx), (millis, datet))
      })
    }

    val endPoints = timeStamps
      .mapPartitions(windowEndPoints, true)

    def applyByArray(f: Iterator[Array[RecordType]] => ResultType)(
                    values: WindowedIterator[((Int, Long), RecordType)],
                    cutIdxs: Iterator[((Int, Int), (Long, DateTime))])// = Iterator[(Interval, ResultType)]
    {
      val (it1, it2) = cutIdxs.duplicate
      val intervals = (it1 zip it1.drop(1)).map(x => new Interval(x._1._2._2, x._2._2._2))
      val windowIdxs = (it2 zip it2.drop(1)).map(x => (x._1._1._2, x._2._1._2))

      val windowValues = values.map(_._2).windows(windowIdxs).map()

    }

    //val zippedRDDs = tiles.zipPartitions(endPoints, true)()



  }




}

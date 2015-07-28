package TsUtils

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
                                                          nColumns: Int,
                                                          timeExtractor: RawType => (DateTime, Array[RecordType]),
                                                          sc: SparkContext,
                                                          memory: Option[Int]) extends Serializable{

  /*
  Initialization phase
   */
  var effectiveLag    = sc.broadcast(if (memory.isDefined) memory.get else 100)
  val nCols           = sc.broadcast(nColumns)
  val nSamples        = sc.broadcast(rawRDD.count())
  val partitionLength = sc.broadcast(nSamples.value.toInt / 8)
  val nPartitions     = sc.broadcast(floor(rawRDD.count() / partitionLength.value).toInt)
  val partitioner     = new TSPartitioner(nPartitions.value)

  val (dataTiles: RDD[Array[RecordType]], timestampTiles: RDD[(Int, (Long, DateTime))]) =
    TimeSeriesHelper.buildTilesFromSyncData(this, rawRDD, timeExtractor)

  /*
  ################################################################

  Accessors
  TODO: modifiy collect once TimeSeries extends RDD in order to remove duplicate keys.

  ################################################################
  */



  /*
  #################################################################

  Computational utils. These will not be declared here in the end.

  #################################################################
   */

  /*
  The compute cross fold is typically used to compute cross correlations
  TODO: Implement a special case when cLeft == cRight
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
      val effectiveSize = min(leftCol.length, partitionLength.value + lag)
      for(rowIdx <- 0 until (effectiveSize - lag)){
        result = foldOp(result, cross(leftCol(rowIdx + lag), rightCol(rowIdx)))
      }
      result
    }

    dataTiles.glom().map(computeCrossFoldArray).fold(zero)(foldOp(_,_))
  }

  /*
  f takes several columns and returns a result (for example linear regression)
  slicer returns true if two timestamps do not belong to the same window.
  For now multiple values will be returned for the overlapping windows. These can
  be unified thanks to a collectAsMap.
  TODO: Return another timeSeries here
   */
  def applyBy[ResultType: ClassTag](f: Seq[Array[RecordType]] => ResultType,
                                    slicer: (DateTime, DateTime) => Boolean)={//: RDD[(Interval, ResultType)] = {

    def windowEndPoints(tsGroup: Iterator[(Int, (Long, DateTime))]): Iterator[(Int, Long, Long)] = {
      val (it1, it2) = tsGroup.duplicate
      (it1 zip it2.zipWithIndex.drop(1))
        .filter({
        case ((pIdx1, (millis1, datet1)), ((pIdx2, (millis2, datet2)), idx2)) => slicer(datet1, datet2)
      })
        .map({
        case ((_, (stopMillis, _)), ((_, (startMillis, _)), startIdx)) => (startIdx, stopMillis, startMillis)
      })
    }

    val endPoints = timestampTiles
      .mapPartitions(windowEndPoints, true)

    endPoints.persist()

    val monitorEndPoints = endPoints.glom.collect

    def applyByWindow(g: Seq[Array[RecordType]] => ResultType)(
      values: Iterator[Array[RecordType]],
      cutIdxs: Iterator[(Int, Long, Long)]) = {//:Iterator[(Interval, ResultType)] = {

      val valueArray = values.toArray
      val cutIdxArray = cutIdxs.toArray

      // Sliding could be used here but case matching will not work later on
      val intervals = (cutIdxArray zip cutIdxArray.drop(1)).map(x => new Interval(x._1._3, x._2._2))
      val windowIdxs = (cutIdxArray zip cutIdxArray.drop(1)).map(x => (x._1._1, x._2._1))

      val valueWindows = windowIdxs
        .map({case (startIdx, stopIdx) => valueArray.map(_.slice(startIdx, stopIdx))})

      (intervals zip valueWindows).map({case(interval, valueWindow) => (interval, g(valueWindow))}).toIterator
    }

    dataTiles.persist()

    val result = dataTiles.zipPartitions(endPoints, true)(applyByWindow(f))

    dataTiles.unpersist()
    endPoints.unpersist()

    result
  }

}

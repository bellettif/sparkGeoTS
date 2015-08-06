package TimeIndex.Containers

import TimeSeriesHelper.{TSInstant, TSInterval}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.math._
import scala.reflect._

/*
Time series factory.
 */
object TimeSeries{

  def apply[RawType: ClassTag, RecordType: ClassTag](
        rawRDD: RDD[RawType],
        nColumns: Int,
        timeExtractor: RawType => (TSInstant, Array[RecordType]),
        sc: SparkContext,
        memoryMillis: Long): TimeSeries[RecordType] ={

    val memory            = sc.broadcast(memoryMillis)
    val nCols             = sc.broadcast(nColumns)
    val nSamples          = sc.broadcast(rawRDD.count())
    val partitionDuration = sc.broadcast(nSamples.value / 8L)
    val nPartitions       = sc.broadcast(floor(rawRDD.count() / partitionDuration.value).toInt)

    val config = new TimeSeriesConfig(
      memory,
      nCols,
      nSamples,
      partitionDuration,
      nPartitions)

    val (partitioner: TSPartitioner, dataTiles: RDD[Array[RecordType]], timestampTiles: RDD[TSInstant]) =
      TimeSeriesHelper.buildTilesFromSyncData(config, rawRDD, timeExtractor)

    val temp = dataTiles.mapPartitionsWithIndex({case (x, y) => Seq((x, y.toSeq.apply(0).size)).toSeq.toIterator}).collectAsMap

    new TimeSeries(config, partitioner, dataTiles, timestampTiles)
  }

  def fromIntervals[RecordType: ClassTag](timeSeriesConfig: TimeSeriesConfig,
                                          partitioner: TSPartitioner,
                                          intervalRDD: RDD[(TSInterval, Array[RecordType])]): TimeSeries[RecordType] = {
    val timestampTiles  = intervalRDD.mapPartitions(_.map(_._1.getEnd), true)
    val dataTiles       = intervalRDD.mapPartitions(_.map(x => Array(x._2: _*)), true)

    new TimeSeries[RecordType](timeSeriesConfig, partitioner, dataTiles, timestampTiles)
  }

}

/**
 * Created by Francois Belletti on 6/24/15.
 */
class TimeSeries[RecordType: ClassTag](val config: TimeSeriesConfig,
                                       val partitioner: TSPartitioner,
                                       val dataTiles: RDD[Array[RecordType]],
                                       val timestampTiles: RDD[TSInstant])
  extends Serializable{

  /*
  ################################################################

  Initialization
  TODO: Make sure that the first range partitionning does follow a chronological order

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
    a cross covariation for example.
    @param: cross: function that multiplies two row elements together. Does not have to be comm or asso.
    @param: foldOp: how to fold results of cross operations. Has to be assoc.
    @param: cLeft: index of the first column.
    @param: cRight: index of the second column.
    @param: lag: value of lag applied to the right column. A positive lag delays the right column w.r.t
     the left one.
    @param: zero: the neutral element to be used in the fold operation.
    @retval: the resulting scalar value.
     */

    def computeCrossFoldArray(partIdx:Int, dataColumns: Iterator[Array[RecordType]]): Iterator[ResultType] ={

      var leftColumn:   Array[RecordType] = null
      var rightColumn:  Array[RecordType] = null

      for((array: Array[RecordType], idx: Int) <- dataColumns.zipWithIndex) {
        if (idx == cLeft)
          leftColumn = array
        if (idx == cRight)
          rightColumn = array
      }

      var result: ResultType = zero

      val effectiveSize = min(leftColumn.length, partitioner.getPartitionSize(partIdx) + lag)
      for(rowIdx <- 0 until (effectiveSize - lag)){
        result = foldOp(result, cross(leftColumn(rowIdx + lag), rightColumn(rowIdx)))
      }
      Seq(result).toIterator
    }

    val temp = dataTiles.glom.collect

    dataTiles.mapPartitionsWithIndex(computeCrossFoldArray, true).fold(zero)(foldOp(_,_))

  }

  /*
  f takes several columns and returns a result (for example linear regression)
  slicer returns true if two timestamps do not belong to the same window.
  For now multiple values will be returned for the overlapping windows. These can
  be unified thanks to a collectAsMap.
  TODO: Return another timeSeries here
   */
  def windowApply[ResultType: ClassTag](f: Array[Array[RecordType]] => Array[ResultType],
                                    slicer: (TSInstant, TSInstant) => Boolean): TimeSeries[ResultType] ={

    /*
    Compute the indices where each window ends
     */
    def windowEndPoints(partitionIdx: Int, tsGroup: Iterator[TSInstant]): Iterator[(Int, TSInstant, TSInstant)] = {
      val (it1, it2) = tsGroup.duplicate
      (it1 zip it2.zipWithIndex.drop(1))
        .filter({
        case (instant1, (instant2, idx2)) => slicer(instant1, instant2)
      })
        .map({
        case (stopInstant, (startInstant, startIdx)) => (startIdx, stopInstant, startInstant)
      })
    }

    val partitionedIntervals = timestampTiles
      .mapPartitionsWithIndex(windowEndPoints, true)

    partitionedIntervals.persist()

    /*
    Helper function designed to apply f to each sliced window.
     */
    def applyByWindow(g: Array[Array[RecordType]] => Array[ResultType])(
      values: Iterator[Array[RecordType]],
      cutIdxs: Iterator[(Int, TSInstant, TSInstant)]): Iterator[(TSInterval, Array[ResultType])] = {

      val valueArray  = values.toArray
      val cutIdxArray = cutIdxs.toArray

      // Sliding could be used here but case matching will not work later on
      val intervals   = (cutIdxArray zip cutIdxArray.drop(1)).map(x => new TSInterval(x._1._3, x._2._2))
      val windowIdxs  = (cutIdxArray zip cutIdxArray.drop(1)).map(x => (x._1._1, x._2._1))

      val valueWindows = windowIdxs
        .map({case (startIdx, stopIdx) => valueArray.map(_.slice(startIdx, stopIdx))})

      (intervals zip valueWindows).map({case(interval, valueWindow) => (interval, g(valueWindow))}).toIterator
    }

    dataTiles.persist()

    /*
    Paritioning is preserved
     */
    val result = dataTiles.zipPartitions(partitionedIntervals, true)(applyByWindow(f))

    dataTiles.unpersist()
    partitionedIntervals.unpersist()

    TimeSeries.fromIntervals(this.config, this.partitioner, result)
  }

  /*
  TODO: distint and collect can be implemented in a smarter fashion
   */

  def distinct(): RDD[(TSInstant, Array[RecordType])] = {

    timestampTiles.zip(dataTiles).distinct()

  }

  def collect(): Array[(TSInstant, Array[RecordType])] = {

    implicit val TSInstantOrdering = new Ordering[TSInstant] {
      override def compare(a: TSInstant, b: TSInstant) =
        a.compareTo(b)
    }

    this.distinct().sortBy(_._1).collect()
  }

}

package TsUtils.HF_estimators

import TsUtils.TimeSeries
import TsUtils.TimeSeriesHelper.TSInstant

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 8/3/15.
 */
class HayashiYoshida[LeftRecordType: ClassTag, RightRecordType: ClassTag](
                 leftTS: TimeSeries[LeftRecordType],
                 rightTS: TimeSeries[RightRecordType]) extends Serializable{


  def computeCrossFoldHY[ResultType: ClassTag](crossCov: ((LeftRecordType, LeftRecordType),
                                                          (RightRecordType, RightRecordType)) => ResultType,
                                               leftVol: ((LeftRecordType, LeftRecordType)) => ResultType,
                                               rightVol: ((RightRecordType, RightRecordType)) => ResultType,
                                               foldOp: (ResultType, ResultType) => ResultType,
                                               zero: ResultType)
                                              (cLeft: Int, cRight: Int, lagMillis: Long): (ResultType, ResultType, ResultType) ={

    val leftTimestamps  = leftTS.timestampTiles
    val leftValues      = leftTS.dataTiles

    val rightTimestamps = rightTS.timestampTiles
    val rightValues     = rightTS.dataTiles

    def computeCrossFoldPartition(partitionLeftTimestamps: Iterator[TSInstant],
                                  partitionRightTimestamps: Iterator[TSInstant],
                                  partitionLeftValues: Iterator[Array[LeftRecordType]],
                                  partitionRightValues: Iterator[Array[RightRecordType]]): Iterator[(ResultType, ResultType, ResultType)] = {

      val leftIntervals: Iterator[(Long, Long)] = partitionLeftTimestamps
        .sliding(2, 1)
        .map(x => (x.head.getMillis - lagMillis, x.last.getMillis - lagMillis))
      val rightIntervals: Iterator[(Long, Long)] = partitionRightTimestamps
        .sliding(2, 1)
        .map(x => (x.head.getMillis, x.last.getMillis))

      val leftDeltas: Iterator[(LeftRecordType, LeftRecordType)] = partitionLeftValues
        .drop(cLeft)
        .next
        .sliding(2, 1)
        .map(x => (x.last, x.head))
      val rightDeltas: Iterator[(RightRecordType, RightRecordType)] = partitionRightValues
        .drop(cRight)
        .next
        .sliding(2, 1)
        .map(x => (x.last, x.head))

      var covariation = zero
      var leftVariation = zero
      var rightVariation = zero

      if (rightIntervals.hasNext) {

        var rightInterval = rightIntervals.next()
        var rightDelta = rightDeltas.next()
        rightVariation = foldOp(rightVariation, rightVol(rightDelta))

        for (leftInterval <- leftIntervals) {

          val leftDelta = leftDeltas.next()
          leftVariation = foldOp(leftVariation, leftVol(leftDelta))

          // Right interval is not completely after left
          while (rightIntervals.hasNext && (rightInterval._1 < leftInterval._2)) {

            if (rightInterval._2 > leftInterval._1) {
              // Right interval is not completely before left
                covariation = foldOp(covariation, crossCov(leftDelta, rightDelta))
            }

            rightInterval   = rightIntervals.next()
            rightDelta      = rightDeltas.next()
            rightVariation  = foldOp(rightVariation, rightVol(rightDelta))

          }

          if(rightInterval._1 < leftInterval._2){
            covariation = foldOp(covariation, crossCov(leftDelta, rightDelta))
          }

        }
      }

      ((leftVariation, rightVariation, covariation) :: Nil).toIterator
    }

    leftTimestamps.zipPartitions(rightTimestamps, leftValues, rightValues, true)(computeCrossFoldPartition)
      .fold((zero, zero, zero))({case ((lv1, rv1, cv1), (lv2, rv2, cv2)) => (foldOp(lv1, lv2), foldOp(rv1, rv2), foldOp(cv1, cv2))})

  }

}

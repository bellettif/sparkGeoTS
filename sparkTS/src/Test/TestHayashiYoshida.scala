package Test

import TsUtils.HF_estimators.HayashiYoshida
import TsUtils.{TimeSeries, TestUtils}
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by Francois Belletti on 8/4/15.
 */
class TestHayashiYoshida extends FlatSpec with Matchers{

  "A time series" should "the sum of its elements when partitioning" in {

    val nColumns      = 10
    val nSamples      = 10000L
    val deltaTMillis  = 1L
    val memory        = 40L

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawTSRDD = TestUtils.getCumOnesRawTsRDD(nColumns, nSamples.toInt, deltaTMillis, sc)

    val timeSeries = TimeSeries[Array[Any], Double](
      rawTSRDD,
      nColumns,
      x => (x.head.asInstanceOf[DateTime], x.drop(1).map(_.asInstanceOf[Double])),
      sc,
      memory
    )

    val HYEstimator = new HayashiYoshida(timeSeries, timeSeries)

    HYEstimator.computeCrossFoldHY[Double](
    {case ((x1, x2), (y1, y2)) => (y2 - y1) * (x2 - x1)},
    {case (x1, x2) => (x1 - x2) * (x1 - x2)},
    {case (x1, x2) => (x1 - x2) * (x1 - x2)},
    _ + _,
    0.0
    )(0, 0, 0L) should be((nSamples - 1, nSamples - 1, nSamples - 1))

  }



}
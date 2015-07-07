package Test

import breeze.linalg._
import TsUtils.{TestUtils, TimeSeries}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalatest._

import scala.math._

/**
 * Created by Francois Belletti on 6/22/15.
 */
class TestTimeSeries extends FlatSpec with Matchers{

  "A time series" should "the sum of its elements when partitioning" in {

    val nColumns = 10
    val nSamples = 1000

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawTSRDD = TestUtils.getOnesRawTsRDD(nColumns, nSamples, sc)

    val timeSeries = new TimeSeries[Array[Any], Double](rawTSRDD,
      x => (x.head.asInstanceOf[DateTime], x.drop(1).map(_.asInstanceOf[Double])),
      sc,
      Some(20)
    )

    // This computes the sum of all elements as a cross fold operation.
    for(i <- 0 until nColumns) {
      timeSeries.computeCrossFold[Double](_ * _, _ + _, 0, i, 0, 0.0) should be(nSamples)
    }

    // This computes the sum of all elements but the lag last ones as a cross fold operation.
    val lag = 5;
    for(i <- 0 until nColumns) {
      timeSeries.computeCrossFold[Double](_ * _, _ + _, 0, i, lag, 0.0) should be(nSamples - lag)
    }

  }



}

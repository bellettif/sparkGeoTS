/**
 * Created by cusgadmin on 6/9/15.
 */

import TsUtils.{TimeSeries, TestUtils}

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.rdd.{RDD, OrderedRDDFunctions}

import org.joda.time.DateTime

import breeze.linalg._

object TestTsDataFrame {

  def main(args: Array[String]): Unit ={

    val nColumns = 10
    val nSamples = 100

    val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc    = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    /*
    val rawTSDataFrame = TestUtils.getRandomTsDataFrame(nColumns, nSamples, sc, sqlContext)

    println(rawTSDataFrame.minTS)
    println(rawTSDataFrame.maxTS)
    */

    val rawTSRDD = TestUtils.getRandomRawTsRDD(nColumns, nSamples, sc)

    val timeSeries = new TimeSeries[Array[Any], Double](rawTSRDD,
      x => (x.head.asInstanceOf[DateTime].getMillis, x.drop(1).map(_.asInstanceOf[Double])),
      sc,
      Some(10)
    )

    timeSeries.timeRDD.take(100).foreach(println)
    println(timeSeries.timeRDD.count())

    println(timeSeries.partitioner.numPartitions)
    println(timeSeries.nPartitions.value)

    timeSeries.augmentedIndexRDD.take(100).foreach(println)
    println(timeSeries.augmentedIndexRDD.count())

    val temp = timeSeries.augmentedIndexRDD.glom().collect

    println("Done")

    /*
    timeSeries.preP`artitionedRDD.take(100).foreach(println)
    println(timeSeries.prePartitionedRDD.count())

    timeSeries.prePartitionedForwardedRDD.take(100).foreach(println)
    println(timeSeries.prePartitionedForwardedRDD.count())

    timeSeries.joinedRDD.take(100).foreach(println)
    println(timeSeries.joinedRDD.count())
    */

  }
}

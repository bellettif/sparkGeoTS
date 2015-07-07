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
    val nSamples = 1000000

    val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc    = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val rawTsRDD = TestUtils.getJodaRandomTsRDD(nColumns, nSamples, sc)

    val timeSeries = new TimeSeries[Array[Any], Double](rawTsRDD,
      x => (x.head.asInstanceOf[DateTime], x.drop(1).map(_.asInstanceOf[Double])),
      sc,
      Some(20)
    )


    val temp = timeSeries.timeStamps.collect()

    def secondSlicer(t1 : DateTime, t2: DateTime): Boolean ={
      t1.secondOfDay() != t2.secondOfDay()
    }

    timeSeries.applyBy(secondSlicer)

    println("Done")


  }
}

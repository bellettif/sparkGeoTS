package ioTools

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import overlapping.timeSeries._

import scala.io
import scala.util.Try

/**
 * Created by Francois Belletti on 9/22/15.
 */
object ReadCsv {

  /**
   * Parse a row and convert timestamp.
   *
   * @param row
   * @param indexCol Index of the column holding the timestamp.
   * @param sep
   * @param dateTimeFormat
   * @return
   */
  def parseRow(
      row: String,
      indexCol: Int,
      sep: String,
      dateTimeFormat: String): Option[(TSInstant, DenseVector[Double])] ={

    val splitRow = row.split(sep).map(_.trim)
    val dataColumns = splitRow.indices.filter(i => i != indexCol).toArray

    Try {
      val timestamp = DateTime.parse(splitRow(indexCol), DateTimeFormat.forPattern(dateTimeFormat))
      val rowData = DenseVector(dataColumns.map(i => splitRow(i).toDouble))
      return Some((new TSInstant(timestamp), rowData))
    }.toOption

  }

  /**
   * Parse a csv file and convert it to a RDD.
   * All rows for which parsing fails will be skipped.
   *
   * @param sc
   * @param filePath
   * @param indexCol
   * @param dateTimeFormat
   * @param header
   * @param sep
   * @return
   */
  def apply(
      sc: SparkContext,
      filePath: String,
      indexCol: Int = 0,
      dateTimeFormat: String = "yyyy-MM-dd HH:mm:ss",
      header: Boolean = true,
      sep: String = ","): (RDD[(TSInstant, DenseVector[Double])], Int, Long) = {

    val data = sc.textFile(filePath)

    val rdd = data
      .map(parseRow(_, indexCol, sep, dateTimeFormat))
      .filter(_.nonEmpty)
      .map(_.get)

    val nSamples = rdd.count

    val temp = rdd.takeSample(true, 1, 42)

    val nDims = rdd.takeSample(true, 1, 42)(0)._2.length

    (rdd, nDims, nSamples)

  }

}

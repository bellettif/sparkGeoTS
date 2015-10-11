package ioTools

import breeze.linalg.DenseVector
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import overlapping.timeSeries._

import scala.io

/**
 * Created by Francois Belletti on 9/22/15.
 */
object ReadCsv {

  def apply(filePath: String, indexCol: Int, dateTimeFormat: String, header: Boolean):
  List[(TSInstant, DenseVector[Double])] = {

    val bufferedSource = io.Source.fromFile(filePath)

    val firstLine   = bufferedSource.getLines().next().split(",")
    val nDims       = firstLine.length - 1
    val dataColumns = firstLine.indices.filter(i => i != indexCol).toArray

    var result = List[(TSInstant, DenseVector[Double])]()

    var first = true

    for (line <- bufferedSource.getLines()) {
      if(first){
        first = false
      }
      if((!header) || (!first)) {
        val row       = line.split(",").map(_.trim)
        val timestamp = DateTime.parse(row(indexCol), DateTimeFormat.forPattern(dateTimeFormat))
        val rowData   = DenseVector(dataColumns.map(i => row(i).toDouble))
        result = (new TSInstant(timestamp), rowData) :: result
      }
    }

    bufferedSource.close()

    result
  }

}

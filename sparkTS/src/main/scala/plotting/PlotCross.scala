package main.scala.plotting

import breeze.linalg._
import breeze.plot._
import org.joda.time.DateTime

/**
  * Created by Francois Belletti on 10/28/15.
  */
object PlotCross {

  def showCrossCorrelation(
      crossCorrelation: Array[DenseMatrix[Double]],
      title: Option[String] = None,
      saveToFile: Option[String] = None): Unit = {

    val h = crossCorrelation.length
    val d = crossCorrelation.head.rows

    val f = Figure()

    val xValues: DenseVector[Double] = linspace(0.0, h.toDouble, h)

    for(i <- 0 until d) {
      for(j <- 0 until d) {

        val p = f.subplot(d, d, i + j * d)
        p += plot(xValues, DenseVector(crossCorrelation.map(x => x(i, j))))

      }
    }

    if(title.isDefined) {
      f.subplot(0).title = title.get
    }

    if(saveToFile.isDefined){
      f.saveas(saveToFile.get)
    }

  }

  def showCrossSpectrum(
     crossSpectrum: Array[Array[DenseVector[Double]]],
     title: Option[String] = None,
     saveToFile: Option[String] = None): Unit = {

    val d = crossSpectrum.length
    val h = crossSpectrum.head.head.size

    val f = Figure()

    val xValues: DenseVector[Double] = linspace(0.0, h.toDouble, h)

    for(i <- 0 until d) {
      for(j <- 0 until d) {

        val p = f.subplot(d, d, i + j * d)
        p += plot(xValues, crossSpectrum(i)(j))

      }
    }

    if(title.isDefined) {
      f.subplot(0).title = title.get
    }

    if(saveToFile.isDefined){
      f.saveas(saveToFile.get)
    }

  }



 }

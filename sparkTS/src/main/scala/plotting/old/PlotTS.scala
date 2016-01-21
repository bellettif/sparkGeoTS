package main.scala.plotting.old

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.plot._
import org.joda.time.DateTime

/**
 * Created by Francois Belletti on 10/28/15.
 */
object PlotTS {

  def showModel(
      modelCoeffs: Array[DenseMatrix[Double]],
      title: Option[String] = None,
      saveToFile: Option[String] = None): Unit ={

    val f = Figure()

    for(i <- modelCoeffs.indices) {

      val p = f.subplot(modelCoeffs.length, 1, i)
      p += image(modelCoeffs(i), GradientPaintScale[Double](-1.0, 1.0))

    }

    if(title.isDefined) {
      f.subplot(0).title = title.get
    }

    if(saveToFile.isDefined){
      f.saveas(saveToFile.get)
    }

  }

  def showCovariance(
      covMatrix: DenseMatrix[Double],
      title: Option[String] = None,
      saveToFile: Option[String] = None): Unit ={

    val f = Figure()

    val p = f.subplot(0)
    p += image(covMatrix.toDenseMatrix)

    if(title.isDefined) {
      f.subplot(0).title = title.get
    }

    if(saveToFile.isDefined){
      f.saveas(saveToFile.get)
    }

  }

  def showProfile(
      profileMatrix: DenseMatrix[Double],
      title: Option[String] = None,
      saveToFile: Option[String] = None): Unit ={

    val f = Figure()

    val p = f.subplot(0)
    p += image(profileMatrix.t)

    p.ylabel = "Space"
    p.xlabel = "Time"

    if(title.isDefined) {
      f.subplot(0).title = title.get
    }

    if(saveToFile.isDefined){
      f.saveas(saveToFile.get)
    }

  }

  def showUnivModel(
      modelCoeffs: Array[DenseVector[Double]],
      title: Option[String] = None,
      saveToFile: Option[String] = None): Unit ={

    val f = Figure()

    for(i <- modelCoeffs.indices) {

      val p = f.subplot(modelCoeffs.length, 1, i)
      p += image(modelCoeffs(i).toDenseMatrix, GradientPaintScale[Double](-1.0, 1.0))

    }

    if(title.isDefined) {
      f.subplot(0).title = title.get
    }

    if(saveToFile.isDefined){
      f.saveas(saveToFile.get)
    }

  }

}

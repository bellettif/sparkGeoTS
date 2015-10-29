package overlapping.timeSeries

import breeze.linalg.{DenseMatrix, min, DenseVector}
import breeze.plot._
import org.apache.spark.rdd.RDD
import overlapping.containers.SingleAxisBlock

/**
 * Created by Francois Belletti on 10/28/15.
 */
object PlotTS {

  def apply(timeSeries: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])],
            title: String = "",
            selectSensors: Option[Array[Int]] = None)
           (implicit tSConfig: TSConfig): Unit = {

    val N = tSConfig.nSamples
    val res = min(N.toDouble, 1600.0)

    /**
     * TODO: incorporate sampling into the overlapping block
     */
    val extracted = timeSeries
      .flatMap(x => x._2.map({case (t, v) => v}).data)
      .sample(false, res / N.toDouble)
      .sortBy(x => x._1)
      .collect()

    val timeVector = DenseVector(extracted.map(_._1.timestamp.getMillis.toDouble))

    val d = extracted.head._2.length

    val f = Figure()

    for (i <- selectSensors.getOrElse(0 until d toArray)) {

      val p = f.subplot(d, 1, i)

      p.ylabel = "sensor " + i
      p.xlabel = "time (ms)"

      val obsVector = DenseVector(extracted.map(_._2(i)))

      p += plot(timeVector, obsVector)
    }

    f.subplot(0).title = title
  }

  def showModel(modelCoeffs: Array[DenseMatrix[Double]], title: String = ""): Unit ={

    val f = Figure()

    for(i <- modelCoeffs.indices) {

      val p = f.subplot(modelCoeffs.length, 1, i)
      p += image(modelCoeffs(i), GradientPaintScale[Double](-1.0, 1.0))

    }

    f.subplot(0).title = title

  }

  def showCovariance(covMatrix: DenseMatrix[Double], title: String = ""): Unit ={

    val f = Figure()

    val p = f.subplot(0)
    p += image(covMatrix.toDenseMatrix, GradientPaintScale[Double](-1.0, 1.0))

    p.title = title

  }

  def showProfile(profileMatrix: DenseMatrix[Double], title: String = ""): Unit ={

    val f = Figure()

    val p = f.subplot(0)
    p += image(profileMatrix.t, GradientPaintScale[Double](-1.0, 1.0))

    p.title = title
    p.ylabel = "Space"
    p.xlabel = "Time"

  }

  def showUnivModel(modelCoeffs: Array[DenseVector[Double]], title: String = ""): Unit ={

    val f = Figure()

    for(i <- modelCoeffs.indices) {

      val p = f.subplot(modelCoeffs.length, 1, i)
      p += image(modelCoeffs(i).toDenseMatrix, GradientPaintScale[Double](-1.0, 1.0))

    }

    f.subplot(0).title = title

  }

}

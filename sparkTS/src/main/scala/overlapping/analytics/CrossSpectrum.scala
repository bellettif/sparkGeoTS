package main.scala.overlapping.analytics

import breeze.linalg._
import breeze.math.Complex
import main.scala.overlapping.containers._

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/10/15.
 */

/**
Here we expect the number of dimensions to be the same for all records.

The autocovoriance is ordered as follows

-modelOrder ... 0 ... modelOrder
  */

object CrossSpectrum{

  def selection[IndexT : TSInstant](bckPadding: IndexT)(
    target: IndexT, aux: IndexT): Boolean = {

    if(implicitly[TSInstant[IndexT]].compare(target, aux) == 1){
      // Target is after aux
      val timeBtw = implicitly[TSInstant[IndexT]].timeBtw(aux, target)
      implicitly[TSInstant[IndexT]].compare(timeBtw, bckPadding) <= 0
    }else{
      // Target is before aux
      val timeBtw = implicitly[TSInstant[IndexT]].timeBtw(target, aux)
      implicitly[TSInstant[IndexT]].compare(timeBtw, bckPadding) <= 0
    }

  }

  def kernel[IndexT : TSInstant](maxLag: Int, d: Int)(
    slice: Array[(IndexT, DenseVector[Double])]): (Array[Array[DenseVector[Complex]]], Long) = {

    val modelWidth = 2 * maxLag + 1

    val result = Array.fill(d){Array.fill(d){DenseVector.zeros[Complex](2 * maxLag + 1)}}

    // The slice is not full size, it shall not be considered
    // so that the normalizing constant is similar for all autocov(h).
    if(slice.length != modelWidth){
      return (result, 0L)
    }

    val window: DenseVector[Double] = (- linspace(-1.0, 1.0, 2 * maxLag + 1) :* linspace(-1.0, 1.0, 2 * maxLag + 1)) :+ 1.0
    window :*= 1.0 / sum(window :* window)

    val ffts = Array.fill(d){DenseVector.zeros[Complex](2 * maxLag + 1)}

    var i, j = 0

    while(i < d){

      ffts(i) = breeze.signal.fourierTr[DenseVector[Double], DenseVector[Complex]](DenseVector(slice.map(_._2(i))) :* window)

      i += 1
    }

    i = 0

    while(i < d){
      j = i

      while(j < d){

        result(i)(j) := ffts(i) :* ffts(j).map(_.conjugate)

        j += 1
      }

      i += 1
    }

    (result, 1L)

  }

  def reduce(
      x: (Array[Array[DenseVector[Complex]]], Long),
      y: (Array[Array[DenseVector[Complex]]], Long)): (Array[Array[DenseVector[Complex]]], Long) ={

    val d = x._1.length
    val result1 = Array.fill(d){Array.fill(d){DenseVector.zeros[Complex](x._1(0)(0).length)}}

    var i, j = 0

    while(i < d){
      j = i

      while(j < d){

        result1(i)(j) := x._1(i)(j) + y._1(i)(j)

        j += 1
      }

      i += 1
    }

    (result1, x._2 + y._2)

  }

  def apply[IndexT : TSInstant : ClassTag](
      timeSeries: SingleAxisVectTS[IndexT],
      maxLag: Int,
      mean: Option[DenseVector[Double]] = None): Array[Array[DenseVector[Complex]]] = {

    val config = timeSeries.config
    val d = config.dim
    val deltaT = config.deltaT

    val padding = implicitly[TSInstant[IndexT]].times(deltaT, maxLag)
    if (implicitly[TSInstant[IndexT]].compare(padding, config.bckPadding) > 0) {
      throw new IndexOutOfBoundsException("Not enough backward padding to support cross spectrum estimation. At least deltaT * maxLag is necessary.")
    }
    if (implicitly[TSInstant[IndexT]].compare(padding, config.fwdPadding) > 0) {
      throw new IndexOutOfBoundsException("Not enough forward padding to support cross spectrum estimation. At least deltaT * maxLag is necessary.")
    }

    def zero: (Array[Array[DenseVector[Complex]]], Long) = (Array.fill(d){Array.fill(d){DenseVector.zeros[Complex](2 * maxLag + 1)}}, 0L)

    val (spectra, counts) = timeSeries.slidingFold(selection(padding))(
        kernel(maxLag, d),
        zero,
        reduce)

    var i, j = 0

    while(i < d){
      j = 0

      while(j < d){

        if(j >= i){
          spectra(i)(j) :*= 1.0 / Complex(counts.toDouble, 0.0)
        }else{
          spectra(i)(j) := spectra(j)(i).map(_.conjugate) :* (1.0 / Complex(counts.toDouble, 0.0))
        }

        j += 1
      }

      i += 1
    }

    spectra

  }

}


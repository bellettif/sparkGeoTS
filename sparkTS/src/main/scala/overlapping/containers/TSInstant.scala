package main.scala.overlapping.containers

import org.joda.time.DateTime


abstract class TSInstant[T] extends Ordering[T] with Serializable{

  def timeBtw(start: T, end: T): T

  def times(x: T, m: Int): T

}

object TSInstant{

  implicit object LongTSInstant extends TSInstant[Long] {

    def compare(x: Long, y: Long): Int = x.compareTo(y)

    def timeBtw(start: Long, end: Long): Long = end - start

    def times(x: Long, m: Int): Long = x * m

  }

  implicit object JodaTSInstant extends TSInstant[DateTime] {

    def compare(x: DateTime, y: DateTime): Int = x.compareTo(y)

    def timeBtw(start: DateTime, end: DateTime): DateTime = new DateTime(end.getMillis - start.getMillis)

    def times(x: DateTime, m: Int): DateTime = new DateTime(x.getMillis * m)

  }

  implicit object DoubleTSInstant extends TSInstant[Double] {

    def compare(x: Double, y: Double): Int = x.compareTo(y)

    def timeBtw(start: Double, end: Double): Double = end - start

    def times(x: Double, m: Int): Double = x * m

  }

}
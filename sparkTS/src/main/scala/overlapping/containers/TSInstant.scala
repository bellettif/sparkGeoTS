package main.scala.overlapping.containers

import org.joda.time.DateTime

/**
 * Ordered timestamps.
 */
abstract class TSInstant[IndexT](val timestamp: IndexT) extends Ordered[TSInstant[IndexT]]{

  def millisTo(that: TSInstant[IndexT]): Long

  def -(that: TSInstant[IndexT]): TSInstant[IndexT]

  def *(that: Int): TSInstant[IndexT]

}

class LongTSInstant(override val timestamp: Long) extends TSInstant[Long](timestamp){

  override def compare(that: TSInstant[Long]): Int = {
    this.timestamp.compareTo(that.timestamp)
  }

  def millisTo(that: TSInstant[Long]): Long = {
    that.timestamp - timestamp
  }

  def-(that: TSInstant[Long]): TSInstant[Long] = {
    new LongTSInstant(timestamp - that.timestamp)
  }

  def*(that: Int): TSInstant[Long] = {
    new LongTSInstant(timestamp * that)
  }

}

class JodaTSInstant(override val timestamp: DateTime) extends TSInstant[DateTime](timestamp){

  override def compare(that: TSInstant[DateTime]): Int = {
    this.timestamp.compareTo(that.timestamp)
  }

  def millisTo(that: TSInstant[DateTime]): Long = {
    that.timestamp.getMillis - timestamp.getMillis
  }

  def-(that: TSInstant[DateTime]): TSInstant[DateTime] = {
    new JodaTSInstant(new DateTime(timestamp.getMillis - that.timestamp.getMillis))
  }

  def*(that: Int): TSInstant[DateTime] = {
    new JodaTSInstant(new DateTime(timestamp.getMillis * that))
  }

}
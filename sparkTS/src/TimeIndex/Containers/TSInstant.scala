package timeIndex.containers

import org.joda.time.DateTime

/**
 * Created by Francois Belletti on 8/12/15.
 */
case class TSInstant(timestamp: DateTime) extends Ordered[TSInstant]{

  override def compare(that: TSInstant): Int = {
    this.timestamp.compareTo(that.timestamp)
  }

}

package TsUtils

import org.joda.time.Period

/**
 * Created by Francois Belletti on 6/22/15.
 */
trait xts {

  def applyBy(period: Period, rolling: Boolean): Any

  def lag(k: Int, oldColumn: String, newColumn: String): Any

}

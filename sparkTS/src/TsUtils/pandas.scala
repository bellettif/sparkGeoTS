package TsUtils

/**
 * Created by Francois Belletti on 6/22/15.
 */
trait pandas {

  def quantile(frac: Double): Any

  def approxQuantile(frac: Double): Any

}

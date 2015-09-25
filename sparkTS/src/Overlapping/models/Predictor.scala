package overlapping.models

/**
 * Created by Francois Belletti on 9/24/15.
 */
trait Predictor[IndexT <: Ordered[IndexT], ValueT, EstimateT]{

  def predict(data: Array[(IndexT, ValueT)]): EstimateT = ???

}

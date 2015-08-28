package overlapping.models.secondOrder

import breeze.linalg.DenseVector

/**
 * Signature of a second order stationary time series (autocovariation matrix, variation)
 */
case class Signature(covariation: DenseVector[Double], variation: Double)

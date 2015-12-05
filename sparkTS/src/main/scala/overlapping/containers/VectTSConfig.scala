package main.scala.overlapping.containers

/**
 * Created by Francois Belletti on 10/14/15.
 */

class VectTSConfig[IndexT <: TSInstant[IndexT]](
    override val nSamples: Long,
    override val deltaT: IndexT,
    override val bckPadding: IndexT,
    override val fwdPadding: IndexT,
    val dim: Int) extends TSConfig[IndexT](nSamples, deltaT, bckPadding, fwdPadding)

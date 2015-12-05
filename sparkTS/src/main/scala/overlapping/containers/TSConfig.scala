package main.scala.overlapping.containers

/**
 * Created by Francois Belletti on 10/14/15.
 */
class TSConfig[IndexT <: TSInstant[IndexT]](
    val nSamples: Long,
    val deltaT: IndexT,
    val bckPadding: IndexT,
    val fwdPadding: IndexT){

    def selection(t1: IndexT, t2: IndexT): Boolean = {

        // t1 is the target (center of the window), t2 the helper (element of the window)
        if(t1.compareTo(t2) > 0){ // Use backward padding
            bckPadding.compareTo(t1 - t2) >= 0
        }else{ // Use forward padding
            fwdPadding.compareTo(t2 - t1) >= 0
        }

    }

}
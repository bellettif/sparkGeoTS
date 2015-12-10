package main.scala.overlapping.containers

/**
 * Created by Francois Belletti on 10/14/15.
 */
class TSConfig[IndexT : TSInstant](
    val nSamples: Long,
    val deltaT: IndexT,
    val bckPadding: IndexT,
    val fwdPadding: IndexT) extends Serializable{

    def selection(t1: IndexT, t2: IndexT): Boolean = {

        // t1 is the target (center of the window), t2 the helper (element of the window)

        if(implicitly[TSInstant[IndexT]].compare(t1, t2) > 0){ // Use backward padding

            implicitly[TSInstant[IndexT]].compare(
                bckPadding,
                implicitly[TSInstant[IndexT]].timeBtw(t2, t1)) >= 0

        }else{ // Use forward padding

            implicitly[TSInstant[IndexT]].compare(
                bckPadding,
                implicitly[TSInstant[IndexT]].timeBtw(t1, t2)) >= 0

        }

    }

}
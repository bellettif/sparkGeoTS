package overlapping.containers.block

import overlapping.{IntervalSize, CompleteLocation}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 8/18/15.
 */
class ColumnFirstBlock[IndexT <: Ordered[IndexT]](
       override val data: Array[(IndexT, Array[Double])],
       override val locations: Array[(CompleteLocation[IndexT])],
       override val signedDistances: Array[(IndexT, IndexT) => Double])
  extends SingleAxisBlock[IndexT, Array[Double]](data, locations, signedDistances){

  lazy val nCols: Int                     = data.head._2.length
  lazy val columns: Array[Array[Double]]  = data.map(_._2).transpose
  lazy val indices: Array[IndexT]         = data.map(_._1)

  def columnSliding[ResultT: ClassTag](size: Array[IntervalSize],
                                       targets: Array[CompleteLocation[IndexT]])
                                      (kernels: Array[(Array[IndexT], Array[Double]) => ResultT]): SingleAxisBlock[IndexT, Array[ResultT]] = {

    if(kernels.length != nCols){
      throw new IndexOutOfBoundsException("Invalid number of kernels")
    }

    val lookAhead = size.head.lookAhead
    val lookBack  = size.head.lookBack

    var begin_index = 0
    var end_index   = 0

    val result: Array[Array[ResultT]] = Array.fill(targets.length)(Array.empty[ResultT])

    for((center_location, i) <- targets.zipWithIndex){
      if(end_index != -1) {

        begin_index = locations.indexWhere(x => signedDistance(x.k, center_location.k) <= lookBack,
          begin_index)

        end_index = locations.indexWhere(x => signedDistance(center_location.k, x.k) >= lookAhead,
          end_index)

        val indexSlice = indices.slice(begin_index, end_index + 1)

        if ((begin_index != -1) && (end_index != -1)){
          result(i) = (0 until nCols).map(c => kernels(c)(indexSlice, columns(c).slice(begin_index, end_index + 1))).toArray
        }

      }
    }

    new SingleAxisBlock[IndexT, Array[ResultT]](targets.map(_.k).zip(result.transpose), targets, signedDistances)

  }


}

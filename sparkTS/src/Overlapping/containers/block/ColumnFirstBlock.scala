package overlapping.containers.block

import overlapping.{IntervalSize, CompleteLocation}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 8/18/15.
 */
class ColumnFirstBlock[IndexT <: Ordered[IndexT] : ClassTag](
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

    val transposedResult: Array[Array[ResultT]]   = result.transpose
    val newData: Array[(IndexT, Array[ResultT])]  = targets.map(_.k).zip(transposedResult)

    new SingleAxisBlock[IndexT, Array[ResultT]](newData, targets, signedDistances)

  }

  def columnSliding[ResultT: ClassTag](size: Array[IntervalSize])
                                          (kernels: Array[(Array[IndexT], Array[Double]) => ResultT]): SingleAxisBlock[IndexT, Array[ResultT]] = {

    columnSliding(size, locations.slice(firstValidIndex, lastValidIndex + 1))(kernels)

  }

  def columnSlidingFold[ResultT: ClassTag](size: Array[IntervalSize],
                                           targets: Array[CompleteLocation[IndexT]])
                                          (kernels: Array[(Array[IndexT], Array[Double]) => ResultT],
                                           zeros: Array[ResultT],
                                           ops: Array[(ResultT, ResultT) => ResultT]): Array[ResultT] = {

    if(kernels.length != nCols){
      throw new IndexOutOfBoundsException("Invalid number of kernels")
    }

    val lookAhead = size.head.lookAhead
    val lookBack  = size.head.lookBack

    var begin_index = 0
    var end_index   = 0

    var result = zeros

    for((center_location, i) <- targets.zipWithIndex){
      if(end_index != -1) {

        begin_index = locations.indexWhere(x => signedDistance(x.k, center_location.k) <= lookBack,
          begin_index)

        end_index = locations.indexWhere(x => signedDistance(center_location.k, x.k) >= lookAhead,
          end_index)

        val indexSlice = indices.slice(begin_index, end_index + 1)

        if ((begin_index != -1) && (end_index != -1)){
          result = result
            .zip((0 until nCols).map(c => kernels(c)(indexSlice, columns(c).slice(begin_index, end_index + 1))).toArray)
            .zipWithIndex
            .map({case ((x, y), c) => ops(c)(x, y)})
        }
      }
    }

    result
  }

}

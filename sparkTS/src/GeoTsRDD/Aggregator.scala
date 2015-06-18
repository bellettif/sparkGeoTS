package GeoTsRDD

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 6/17/15.
 */
case class Aggregator[AttribType, MergeType](
    initializer: AttribType => MergeType,
    valueMerger: (MergeType, AttribType) => MergeType,
    valueCombiner: (MergeType, MergeType) => MergeType){}

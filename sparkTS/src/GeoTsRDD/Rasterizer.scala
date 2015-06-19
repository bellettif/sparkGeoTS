package GeoTsRDD

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 6/17/15.
 */

/*
TODO: Internal RDD as IndexedRDD (mesh index for spatial data)
TODO: Convert to DataFrame representation
 */
case class Rasterizer[LocType, AttribType: ClassTag, MergeType](
  private val grid: LocType => Long,
  private val aggregator: Aggregator[AttribType, MergeType],
  private val postMapper: ((Long, MergeType)) => (Long, AttribType),
  private val rawData: RDD[LocEvent[LocType, AttribType]]) {

  private val rasters: RDD[(Long, AttribType)] = rawData
    .map(x =>(grid(x.getLoc()), x.getAttrib()))
    .combineByKey(aggregator.initializer, aggregator.valueMerger, aggregator.valueCombiner)
    .map(postMapper)

  def persist() = rasters.persist()

  def getValueAtLoc(loc: LocType): Seq[AttribType] ={
    rasters.lookup(grid(loc))
  }

  def collectRasters() = rasters.collect()

  def collectAsMap() = rasters.collectAsMap()

}

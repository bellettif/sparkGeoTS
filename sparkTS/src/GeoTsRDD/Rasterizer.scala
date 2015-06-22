package GeoTsRDD

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.sql._

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 6/17/15.
 */

/*
TODO: Internal RDD as IndexedRDD (mesh index for spatial data)
TODO: Convert to DataFrame representation
 */
case class Rasterizer[LocType, HashedType: ClassTag, AttribType: ClassTag, MergeType](
  private val grid: LocType => HashedType,
  private val aggregator: Aggregator[AttribType, MergeType],
  private val postMapper: ((HashedType, MergeType)) => (HashedType, AttribType),
  private val rawData: RDD[LocEvent[LocType, AttribType]]) {

  val rasters: RDD[(HashedType, AttribType)] = rawData
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

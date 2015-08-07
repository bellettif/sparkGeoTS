package geoTsRDD

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 6/17/15.
 *
 * This class computes a raster version of any dataFrame
 * with respect to a spatio temporal index grid
 * (or any other kind of grid) held in the grid function.
 *
 * @constructor Create a new raster from a spatial grid and raw data
 * @param grid The spatial gridding function (will return a hashed index to a location).
 * @param aggregator The grid aggregating function (given a set of points with attributes will return
 *                  their sum and count for example).
 * @param postMapper Computes the terminal value (for example sum / count for averaging).
 * @param rawData A RDD of events with locations and attributes.
 *
 * @todo: Internal RDD as IndexedRDD (mesh index for spatial data)
 * @todo: Convert to DataFrame representation
 * @todo: In raw data location of event should be used as a key (for better partitioning)
 *
 */
case class Rasterizer[LocType: ClassTag, HashedType: ClassTag, AttribType: ClassTag, MergeType: ClassTag](
  private val grid: LocType => HashedType,
  private val aggregator: Aggregator[AttribType, MergeType],
  private val postMapper: ((HashedType, MergeType)) => (HashedType, AttribType),
  private val rawData: RDD[LocEvent[LocType, AttribType]])
{

  /*
  * rasters member is publicly exposed (it is immutable anyway)
   */
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

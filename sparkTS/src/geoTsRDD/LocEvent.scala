package geoTsRDD

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 6/17/15.
 */
case class LocEvent[LocType, AttribType](
  private val loc: LocType, private val attrib: AttribType) {

  def getLoc(): LocType ={
    loc
  }

  def getAttrib(): AttribType ={
    attrib
  }

}

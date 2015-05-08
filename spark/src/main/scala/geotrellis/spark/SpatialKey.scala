package geotrellis.spark

import spray.json._
import spray.json.DefaultJsonProtocol._

/** A SpatialKey designates the spatial positioning of a layer's tile. */
case class SpatialKey(col: Int, row: Int) extends Product2[Int, Int] {
  def _1 = col
  def _2 = row
}

object SpatialKey {
  implicit object SpatialComponent extends IdentityComponent[SpatialKey]

  implicit def tupToKey(tup: (Int, Int)): SpatialKey =
    SpatialKey(tup._1, tup._2)

  implicit def keyToTup(key: SpatialKey): (Int, Int) =
    (key.col, key.row)

  implicit def ordering[A <: SpatialKey]: Ordering[A] =
    Ordering.by(sk => (sk.col, sk.row))

  implicit object SpatialKeyFormat extends RootJsonFormat[SpatialKey] {
    def write(key: SpatialKey) =
      JsObject(
        "col" -> JsNumber(key.col),
        "row" -> JsNumber(key.row)
      )

    def read(value: JsValue): SpatialKey =
      value.asJsObject.getFields("col", "row") match {
        case Seq(JsNumber(col), JsNumber(row)) =>
          SpatialKey(col.toInt, row.toInt)
        case _ =>
          throw new DeserializationException("SpatialKey expected")
      }
  }

  implicit object Boundable extends Boundable[SpatialKey] {
    def minBound(a: SpatialKey, b: SpatialKey) = {
      SpatialKey(math.min(a.col, b.col), math.min(a.row, b.row))
    }    
    def maxBound(a: SpatialKey, b: SpatialKey) = {
      SpatialKey(math.max(a.col, b.col), math.max(a.row, b.row))
    }

    def getKeyBounds(rdd: RasterRDD[SpatialKey]): KeyBounds[SpatialKey] = {
      val md = rdd.metaData
      val gb = md.gridBounds
      KeyBounds(
        SpatialKey(gb.colMin, gb.rowMin),
        SpatialKey(gb.colMax, gb.rowMax))    
    }
  }
}

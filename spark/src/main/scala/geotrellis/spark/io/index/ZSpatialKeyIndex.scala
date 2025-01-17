package geotrellis.spark.io.index

import geotrellis.spark._
import geotrellis.index.zcurve._

class ZSpatialKeyIndex() extends KeyIndex[SpatialKey] {
  private def toZ(key: SpatialKey): Z2 = Z2(key.col, key.row)

  def toIndex(key: SpatialKey): Long = toZ(key).z

  def indexRanges(keyRange: (SpatialKey, SpatialKey)): Seq[(Long, Long)] =
    Z2.zranges(toZ(keyRange._1), toZ(keyRange._2))
}

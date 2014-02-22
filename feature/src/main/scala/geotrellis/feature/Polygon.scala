package geotrellis.feature

import com.vividsolutions.jts.{geom => jts}
import GeomFactory._

object Polygon {

  implicit def jtsToPolygon(geom: jts.Polygon): Polygon =
    Polygon(geom)

  def apply(exterior: Line): Polygon =
    apply(exterior, Set())

  def apply(exterior: Line, holes:Set[Line]): Polygon = {
    if(!exterior.isClosed) {
      sys.error(s"Cannot create a polygon with unclosed exterior: $exterior")
    }

    if(exterior.points.length < 4) {
      sys.error(s"Cannot create a polygon with exterior with less that 4 points: $exterior")
    }

    val extGeom = factory.createLinearRing(exterior.geom.getCoordinates)

    val holeGeoms = (
      for (hole <- holes) yield {
        if (!hole.isClosed) {
          sys.error(s"Cannot create a polygon with an unclosed hole: $hole")
        } else {
          if (hole.points.length < 4)
            sys.error(s"Cannot create a polygon with a hole with less that 4 points: $hole")
          else
            factory.createLinearRing(hole.geom.getCoordinates)
        }
      }).toArray

    factory.createPolygon(extGeom, holeGeoms)
  }

}

case class Polygon(geom: jts.Polygon) extends Geometry {

  assert(!geom.isEmpty)

  lazy val isRectangle: Boolean = geom.isRectangle

  lazy val area: Double = geom.getArea

  lazy val exterior = Line(geom.getExteriorRing)

  lazy val boundary: PolygonBoundaryResult =
    geom.getBoundary

  lazy val vertices: PointSet =
    geom.getCoordinates

  lazy val boundingBox: Option[Polygon] =
    if (geom.isEmpty) None else Some(geom.getEnvelope.asInstanceOf[Polygon])

  lazy val perimeter: Double =
    geom.getLength

  // -- Intersection

  def &(p: Point) = intersection(p)
  def intersection(p: Point): PointIntersectionResult =
    p.intersection(this)

  def &(l: Line) = intersection(l)
  def intersection(l: Line): PolygonLineIntersectionResult =
    l.intersection(this)

  def &(p: Polygon) = intersection(p)
  def intersection(p: Polygon): PolygonPolygonIntersectionResult =
    geom.intersection(p.geom)

  def &(ps: PointSet) = intersection(ps)
  def intersection(ps: PointSet): PointSetIntersectionResult =
    geom.intersection(ps.geom)

  def &(ls: LineSet) = intersection(ls)
  def intersection(ls: LineSet): LineSetIntersectionResult =
    geom.intersection(ls.geom)

  def &(ps: PolygonSet) = intersection(ps)
  def intersection(ps: PolygonSet): PolygonSetIntersectionResult =
    geom.intersection(ps.geom)

  // -- Union

  def |(p: Point) = union(p)
  def union(p: Point): PolygonXUnionResult =
    p.union(this)

  def |(l:Line) = union(l)
  def union(l: Line): PolygonXUnionResult =
    l.union(this)

  def |(p:Polygon) = union(p)
  def union(p: Polygon): PolygonPolygonUnionResult =
    geom.union(p.geom)

  def |(ps: PointSet) = union(ps)
  def union(ps: PointSet): PolygonXUnionResult =
    geom.union(ps.geom)

  def |(ls: LineSet) = union(ls)
  def union(ls: LineSet): PolygonXUnionResult =
    geom.union(ls.geom)

  def |(ps: PolygonSet) = union(ps)
  def union(ps: PolygonSet): PolygonPolygonUnionResult =
    geom.union(ps.geom)

  // -- Difference

  def -(p: Point) = difference(p)
  def difference(p: Point): PolygonXDifferenceResult =
    geom.difference(p.geom)

  def -(l: Line) = difference(l)
  def difference(l: Line): PolygonXDifferenceResult =
    geom.difference(l.geom)

  def -(p: Polygon) = difference(p)
  def difference(p: Polygon): PolygonPolygonDifferenceResult =
    geom.difference(p.geom)

  def -(ps: PointSet) = difference(ps)
  def difference(ps: PointSet): PolygonXDifferenceResult =
    geom.difference(ps.geom)

  def -(ls: LineSet) = difference(ls)
  def difference(ls: LineSet): PolygonXDifferenceResult =
    geom.difference(ls.geom)

  def -(ps: PolygonSet) = difference(ps)
  def difference(ps: PolygonSet): PolygonPolygonDifferenceResult =
    geom.difference(ps.geom)

  // -- SymDifference

  def symDifference(p: Point): PointPolygonSymDifferenceResult =
    p.symDifference(this)

  def symDifference(l: Line): LinePolygonSymDifferenceResult =
    l.symDifference(this)

  def symDifference(p: Polygon): PolygonPolygonSymDifferenceResult =
    geom.symDifference(p.geom)

  // -- Buffer

  def buffer(d: Double): Polygon =
    geom.buffer(d).asInstanceOf[Polygon]

  // -- Predicates

  def contains(p: Point): Boolean =
    geom.contains(p.geom)

  def contains(l: Line): Boolean =
    geom.contains(l.geom)

  def contains(p: Polygon): Boolean =
    geom.contains(p.geom)

  def within(p: Polygon): Boolean =
    geom.within(p.geom)

  def crosses(p: Point): Boolean =
    geom.crosses(p.geom)

  def crosses(l: Line): Boolean =
    geom.crosses(l.geom)

  def overlaps(p: Polygon): Boolean =
    geom.overlaps(p.geom)

}

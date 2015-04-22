package geotrellis.spark.io.index.zcurve

import scala.collection.immutable.TreeSet
import org.scalatest._
import geotrellis.spark.SpatialKey

class ZSpatialKeyIndexSpec extends FunSpec with Matchers {

  val UpperBound = 4

  describe("ZSpatialKeyIndex test") {
    it("generates an index from a SpatialKey"){
    
     val zsk = new ZSpatialKeyIndex()
     var i = 0
     var j = 0
     var ts: TreeSet[Long] = TreeSet()

     while (i < UpperBound) {
      j=0
        while(j < UpperBound){

          var idx = zsk.toIndex(i,j)
          var x: Option[Long] = ts.find(y => y == idx)
          if (x.isEmpty){
            ts = ts + idx //add element exactly once
          }else{
            //throw error
            println("=============ERROR============")
          }
       j+=1
      }
      i+=1
     }
     ts.size should be (UpperBound * UpperBound)
    }

    it("generates a Seq[(Long, Long)] from a keyRange (SpatialKey,SpatialKey)"){
     val zsk = new ZSpatialKeyIndex()
     var idx = zsk.indexRanges(SpatialKey(0,0), SpatialKey(10,10))

    }


  }
}


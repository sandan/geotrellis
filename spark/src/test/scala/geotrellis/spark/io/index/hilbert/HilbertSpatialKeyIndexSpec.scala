package geotrellis.spark.io.index.hilbert

import org.scalatest._
import scala.collection.immutable.TreeSet
import geotrellis.spark.SpatialKey

class HilbertSpatialKeyIndexSpec extends FunSpec with Matchers{

  val UpperBound: Int = 64

  describe("HilbertSpatialKeyIndex tests"){

    it("Generates a Long index given a SpatialKey"){
     val hilbert = HilbertSpatialKeyIndex(SpatialKey(0,0), SpatialKey(UpperBound,UpperBound), 6) //what are the SpatialKeys used for?

       var i = 0
       var j = 0
       var ts: TreeSet[Long] = TreeSet()

       while (i < UpperBound) {
        j=0
          while(j < UpperBound){

            var idx = hilbert.toIndex(SpatialKey(j,i))
            var x: Option[Long] = ts.find(y => y == idx)

            x.isEmpty should be (true) //add element exactly once
            ts = ts + idx

            j+=1
        }
        i+=1
       }

       //check size
       ts.size should be (UpperBound * UpperBound)

       //check for consecutivity
       val itr: Iterator[Long] = ts.iterator
       var s = itr.next
       while(itr.hasNext){
        var t = itr.next
        t should be (s+1)
        s = t
       }

     
    }

    it("generates hand indexes you can hand check 2x2"){
     val hilbert = HilbertSpatialKeyIndex(SpatialKey(0,0), SpatialKey(UpperBound,UpperBound), 2) //what are the SpatialKeys used for?
     //right oriented
     hilbert.toIndex(SpatialKey(0,0)) should be (0) // (0,0)
     hilbert.toIndex(SpatialKey(1,0)) should be (1) // (0,1)
     hilbert.toIndex(SpatialKey(0,1)) should be (3) // (1,0)
     hilbert.toIndex(SpatialKey(1,1)) should be (2) // (1,1)
    }

    it("generates hand indexes you can hand check 4x4"){
     val hilbert = HilbertSpatialKeyIndex(SpatialKey(0,0), SpatialKey(UpperBound,UpperBound), 2) //what are the SpatialKeys used for?
     val grid = List[SpatialKey]( SpatialKey(0,0), SpatialKey(1,0), SpatialKey(1,1), SpatialKey(0,1), 
                                  SpatialKey(0,2), SpatialKey(0,3), SpatialKey(1,3), SpatialKey(1,2), 
                                  SpatialKey(2,2), SpatialKey(2,3), SpatialKey(3,3), SpatialKey(3,2), 
                                  SpatialKey(3,1), SpatialKey(2,1), SpatialKey(2,0), SpatialKey(3,0)
                                )
     for{i<- 0 to 15}
	hilbert.toIndex(grid(i)) should be (i)


    }

    it("Generates a Seq[(Long,Long)] given a key range (SpatialKey,SpatialKey)"){


    }


  }
}

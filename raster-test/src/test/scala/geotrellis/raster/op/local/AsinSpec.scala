/*
 * Copyright (c) 2014 Azavea.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.raster.op.local

import geotrellis.raster._

import org.scalatest._

import geotrellis.testkit._

class AsinSpec extends FunSpec
                  with Matchers
                  with TestEngine
                  with TileBuilders {
  describe("ArcSin") {
    it("finds arcsin of a double raster") {
      val rasterData = Array(
        0.0, 0.5, 1/math.sqrt(2),   math.sqrt(3)/2, 1.0, Double.NaN,   0, 0, 0,
        0.0,-0.5,-1/math.sqrt(2),  -math.sqrt(3)/2,-1.0, Double.NaN,   0, 0, 0,

        0.0, 0.5, 1/math.sqrt(2),   math.sqrt(3)/2, 1.0, Double.NaN,   0, 0, 0,
        0.0,-0.5,-1/math.sqrt(2),  -math.sqrt(3)/2,-1.0, Double.NaN,   0, 0, 0
      )
      val rs = createTile(rasterData, 9, 4)
      val result = rs.localAsin()
      val expectedAngles = List( 0.0,    1.0/6,  1.0/4,
                                 1.0/3,  0.5,    Double.NaN,
                                 0.0,    0.0,    0.0,

                                -0.0,   -1.0/6, -1.0/4,
                                -1.0/3, -0.5,   -Double.NaN,
                                -0.0,   -0.0,   -0.0,

                                 0.0,    1.0/6,  1.0/4,
                                 1.0/3,  0.5,    Double.NaN,
                                 0.0,    0.0,    0.0,
                                                             
                                -0.0,   -1.0/6, -1.0/4,
                                -1.0/3, -0.5,   -Double.NaN,
                                -0.0,   -0.0,   -0.0
                           ).map(x => math.Pi * x)
      val width = 9
      val height = 4
      val len = expectedAngles.length
      for (y <- 0 until height) {
        for (x <- 0 until width) {
          val angle = result.getDouble(x, y)
          val i = (y*width + x) % len
          val expected = expectedAngles(i)
          val epsilon = math.ulp(angle)
          if (x == 5) {
            angle.isNaN should be (true)
          } else {
            angle should be (expected +- epsilon)
          }
        }
      }
    }

    it("is NaN when the absolute value of thecell of a double raster > 1") {
      val rasterData = Array(
         2.6,-2.6, 2.6,  -2.6, 2.6,-2.6,  2.6,-2.6, 2.6,
        -2.6, 2.6,-2.6,   2.6,-2.6, 2.6, -2.6, 2.6,-2.6,

         2.6,-2.6, 2.6,  -2.6, 2.6,-2.6,  2.6,-2.6, 2.6,
        -2.6, 2.6,-2.6,   2.6,-2.6, 2.6, -2.6, 2.6,-2.6
      )
      val rs = createTile(rasterData, 9, 4)
      val result = rs.localAsin()
      for (y <- 0 until 4) {
        for (x <- 0 until 9) {
          result.getDouble(x, y).isNaN should be (true)
        }
      }
    }

    it("finds arcsin of an int raster") {
      val rasterData = Array(
         0, 0, 0,   0, 0, 0,   0, 0, 0,
         1, 1, 1,   1, 1, 1,   1, 1, 1,
        -1,-1,-1,  -1,-1,-1,  -1,-1,-1,

         2, 2, 2,   2, 2, 2,   2, 2, 2,
        -2,-2,-2,  -2,-2,-2,  -2,-2,-2,
        NODATA, NODATA, NODATA, NODATA, NODATA, NODATA, NODATA, NODATA, NODATA
      )
      val expectedAngles = Array(0.0, 0.5,-0.5,
                                 Double.NaN, Double.NaN,  Double.NaN)
                            .map(x => x * math.Pi)
      val rs = createTile(rasterData, 9, 6)
      val result = rs.localAsin()
      for (y <- 0 until 3) {
        for (x <- 0 until 9) {
          val cellValue = result.getDouble(x, y)
          val epsilon = math.ulp(cellValue)
          val expected = expectedAngles(y)
          cellValue should be (expected +- epsilon)
        }
      }
      for (y <- 3 until 6) {
        for (x <- 0 until 9) {
          result.getDouble(x, y).isNaN should be (true)
        }
      }
    }
  }
}

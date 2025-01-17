/*
 * Copyright (c) 2014 DigitalGlobe.
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

package geotrellis.spark.op.local

import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.RasterRDD
import geotrellis.spark.testfiles._

import org.scalatest.FunSpec

class AndSpec extends FunSpec
    with TestEnvironment
    with TestFiles
    with RasterRDDMatchers
    with OnlyIfCanRunSpark {
  describe("And Operation") {
    ifCanRunSpark {
      val ones = AllOnesTestFile
      val twos = AllTwosTestFile
      val hundreds = AllHundredsTestFile

      val onesST = AllOnesSpaceTime
      val twosST = AllTwosSpaceTime
      val hundredsST = AllHundredsSpaceTime

      it("should and a raster with a constant") {
        val res = ones & 1

        rasterShouldBe(res, (1, 1))
        rastersShouldHaveSameIdsAndTileCount(ones, res)
      }

      it("should and a spacetime raster with a constant") {
        val res = onesST & 1

        rasterShouldBe(res, (1, 1))
        rastersShouldHaveSameIdsAndTileCount(onesST, res)
      }

      it("should and a constant with a raster") {
        val res = 1 &: ones

        rasterShouldBe(res, (1, 1))
        rastersShouldHaveSameIdsAndTileCount(ones, res)
      }

      it("should and three different rasters") {
        val res = ones & twos & hundreds

        rasterShouldBe(res, (0, 0))
        rastersShouldHaveSameIdsAndTileCount(ones, res)
      }

      it("should and a constant with a spacetime raster") {
        val res = 1 &: onesST

        rasterShouldBe(res, (1, 1))
        rastersShouldHaveSameIdsAndTileCount(onesST, res)
      }

      it("should and three different rasters as a seq") {
        val res = ones & Seq(twos, hundreds)

        rasterShouldBe(res, (0, 0))
        rastersShouldHaveSameIdsAndTileCount(ones, res)
      }

      it("should and three different spacetime rasters as a seq") {
        val res = onesST & Seq(twosST, hundredsST)

        rasterShouldBe(res, (0, 0))
        rastersShouldHaveSameIdsAndTileCount(onesST, res)
      }
    }
  }
}

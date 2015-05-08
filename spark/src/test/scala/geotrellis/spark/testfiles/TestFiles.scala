package geotrellis.spark.testfiles

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.spark._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._

object TestFiles extends Logging {
  val ZOOM_LEVEL = 8

  def catalogPath(implicit sc: SparkContext): Path = {
    val localFS = new Path(System.getProperty("java.io.tmpdir")).getFileSystem(sc.hadoopConfiguration)
    new Path(localFS.getWorkingDirectory, "src/test/resources/test-catalog")
  }

  def catalog(implicit sc: SparkContext): HadoopRasterCatalog = {
    val conf = sc.hadoopConfiguration
    val localFS = catalogPath.getFileSystem(sc.hadoopConfiguration)
    val needGenerate = !localFS.exists(catalogPath)

    val catalog = HadoopRasterCatalog(catalogPath)

    if (needGenerate) {
      logInfo(s"test-catalog empty, generating at $catalogPath")
      GenerateTestFiles.generate(catalog)
    }

    catalog
  }
}

trait TestFiles { self: OnlyIfCanRunSpark =>
  lazy val spatialReader = TestFiles.catalog.reader[SpatialKey]
  lazy val spaceTimeReader = TestFiles.catalog.reader[SpaceTimeKey]

  def spatialTestFile(layerName: String): RasterRDD[SpatialKey] = {
    spatialReader.read(LayerId(layerName, TestFiles.ZOOM_LEVEL)).cache
  }

  def spaceTimeTestFile(layerName: String): RasterRDD[SpaceTimeKey] = {
    spaceTimeReader.read(LayerId(layerName, TestFiles.ZOOM_LEVEL)).cache
  }

  def AllOnesTestFile =
    spatialTestFile("all-ones")

  def AllTwosTestFile =
    spatialTestFile("all-twos")

  def AllHundredsTestFile =
    spatialTestFile("all-hundreds")

  def IncreasingTestFile =
    spatialTestFile("increasing")

  def DecreasingTestFile =
    spatialTestFile("decreasing")

  def EveryOtherUndefinedTestFile =
    spatialTestFile("every-other-undefined")

  def EveryOther0Point99Else1Point01TestFile =
    spatialTestFile("every-other-0.99-else-1.01")

  def EveryOther1ElseMinus1TestFile =
    spatialTestFile("every-other-1-else-1")

  def Mod10000TestFile =
    spatialTestFile("mod-10000")

  def AllOnesSpaceTime =
    spaceTimeTestFile("spacetime-all-ones")

  def AllTwosSpaceTime =
    spaceTimeTestFile("spacetime-all-twos")

  def AllHundredsSpaceTime =
    spaceTimeTestFile("spacetime-all-hundreds")

  /** Coordinates are CCC,RRR.TTT where C = column, R = row, T = time (year in 2010 + T).
    * So 34,025.004 would represent col 34, row 25, year 2014
    */
  def CoordinateSpaceTime =
    spaceTimeTestFile("spacetime-coordinates")
}

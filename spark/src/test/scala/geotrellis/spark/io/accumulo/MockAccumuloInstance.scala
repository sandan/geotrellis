package geotrellis.spark.io.accumulo

import geotrellis.spark._
import geotrellis.spark.tiling._
import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.mapreduce.{InputFormatBase, AccumuloInputFormat, AccumuloOutputFormat}
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.mapreduce.lib.util.{ConfiguratorBase => CB}
import org.apache.accumulo.core.data.{Value, Key, Mutation}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import com.typesafe.config.{ConfigFactory,Config}

object MockAccumuloInstance { def apply(): MockAccumuloInstance = new MockAccumuloInstance }

class MockAccumuloInstance() extends AccumuloInstance {
  val instance: Instance = new MockInstance("fake")
  val user = "root"
  val token = new PasswordToken("")
  val connector = instance.getConnector(user, token)
  val instanceName = "fake"

  def setAccumuloConfig(conf: Configuration): Unit = {
    CB.setMockInstance(classOf[AccumuloInputFormat], conf, instanceName)
    CB.setMockInstance(classOf[AccumuloOutputFormat], conf, instanceName)
    CB.setConnectorInfo(classOf[AccumuloInputFormat], conf, user, token)
    CB.setConnectorInfo(classOf[AccumuloOutputFormat], conf, user, token)
  }
}

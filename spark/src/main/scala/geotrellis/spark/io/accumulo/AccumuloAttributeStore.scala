package geotrellis.spark.io.accumulo

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.json._

import spray.json._
import DefaultJsonProtocol._

import scala.collection.JavaConversions._

import org.apache.spark.Logging
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.data._
import org.apache.hadoop.io.Text

object AccumuloAttributeStore { 
  def apply(connector: Connector, attributeTable: String): AccumuloAttributeStore =
    new AccumuloAttributeStore(connector, attributeTable)
}

class AccumuloAttributeStore(connector: Connector, val attributeTable: String) extends AttributeStore with Logging {
  type ReadableWritable[T] = RootJsonFormat[T]

  //create the attribute table if it does not exist
  {
    val ops = connector.tableOperations()
    if (!ops.exists(attributeTable))
      ops.create(attributeTable)
  }

  private def fetch(layerId: Option[LayerId], attributeName: String): Vector[Value] = {
    val scanner  = connector.createScanner(attributeTable, new Authorizations())
    layerId.map { id => 
      scanner.setRange(new Range(new Text(id.toString)))
    }    
    scanner.fetchColumnFamily(new Text(attributeName))
    scanner.iterator.toVector.map(_.getValue)
  }

  def read[T: RootJsonFormat](layerId: LayerId, attributeName: String): T = {
    val values = fetch(Some(layerId), attributeName)

    if(values.size == 0) {
      sys.error(s"Attribute $attributeName not found for layer $layerId")
    } else if(values.size > 1) {
      sys.error(s"Multiple attributes found for $attributeName for layer $layerId")
    } else {
      values.head.toString.parseJson.convertTo[(LayerId, T)]._2
    }
  }

  def readAll[T: RootJsonFormat](attributeName: String): Map[LayerId,T] = {
    fetch(None, attributeName)
      .map{ _.toString.parseJson.convertTo[(LayerId, T)] }
      .toMap
  }

  def write[T: RootJsonFormat](layerId: LayerId, attributeName: String, value: T): Unit = {
    val mutation = new Mutation(layerId.toString)
    mutation.put(
      new Text(attributeName), new Text(), System.currentTimeMillis(),
      new Value((layerId, value).toJson.compactPrint.getBytes)
    )

    connector.write(attributeTable, mutation)
  }

}

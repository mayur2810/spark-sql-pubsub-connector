package org.apache.spark.sql.connector.pubsub

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.pubsub
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.{HashMap => JavaHashMap}
import scala.collection.JavaConverters.{mapAsScalaMapConverter, setAsJavaSetConverter}

class PubsubTable(tableOptions: CaseInsensitiveStringMap)
  extends Table with SupportsRead {

  override def name(): String = "PubsubTable"

  override def schema(): StructType = pubsub.PubsubReadSchema

  override def capabilities(): util.Set[TableCapability] = Set[TableCapability](
    TableCapability.MICRO_BATCH_READ
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val mergedOptions = new JavaHashMap[String, String](tableOptions.asCaseSensitiveMap())
    mergedOptions.putAll(options.asCaseSensitiveMap())
    new PubsubScanBuilder(schema(), validateAndInitReadOptions(mergedOptions.asScala.toMap))
  }

}

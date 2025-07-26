package org.apache.spark.sql.connector.pubsub

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

class PubsubScanBuilder(schema: StructType, options: PubsubReadOptions) extends ScanBuilder  {
  override def build(): Scan = new PubsubScan(schema, options)
}

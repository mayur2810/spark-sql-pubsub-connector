package org.apache.spark.sql.connector.pubsub

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.types.StructType

class PubsubScan(schema: StructType, options: PubsubReadOptions) extends Scan{
  override def readSchema(): StructType = schema

  override def toMicroBatchStream(checkpointLocation: String): PubsubMicroBatchStream = {
    new PubsubMicroBatchStream(schema, options, checkpointLocation)
  }

  override def columnarSupportMode(): Scan.ColumnarSupportMode = Scan.ColumnarSupportMode.UNSUPPORTED
}

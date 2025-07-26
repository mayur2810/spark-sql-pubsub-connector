package org.apache.spark.sql.connector.pubsub

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PubsubTableProvider extends Logging
  with SimpleTableProvider
  with StreamSinkProvider
  with DataSourceRegister {

  override def getTable(options: CaseInsensitiveStringMap): Table = new PubsubTable(options)

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {

    if (outputMode != OutputMode.Append())
      throw new IllegalStateException(s"OutputMode $outputMode not supported.")

    PubsubSink(sqlContext, validateAndInitWriteOption(parameters))
  }

  override def shortName(): String = "pubsub"

}

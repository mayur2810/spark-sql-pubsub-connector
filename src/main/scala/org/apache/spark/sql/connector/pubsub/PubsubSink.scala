package org.apache.spark.sql.connector.pubsub

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.pubsub
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SQLContext}

case class PubsubSink(sqlContext: SQLContext, pubsubWriteOptions: PubsubWriteOptions) extends Sink with Logging {


  @volatile private var latestBatchId = -1L


  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    {
      if (batchId <= latestBatchId) {
        logInfo(s"Skipping already committed batch $batchId")
      } else {

        logInfo(s"Writing data to Pubsub topic: ${pubsubWriteOptions.getTopic} for batchId: " + batchId)

        if (!pubsub.PubsubWriteSchema.fields.forall(field =>
          data.schema.fields.exists(inputSchemaField => inputSchemaField.name == field.name
            && inputSchemaField.dataType == field.dataType)))
          throw new IllegalStateException(s"The input data schema must match to ${pubsub.PubsubWriteSchema}")

        pubsubWriteOptions.orderingKey.fold(()) { key =>
          if (!data.schema.fields.exists(_.name == key))
            throw new IllegalStateException(s"Ordering key column $key not found in schema")
          data.schema.fields.find(_.name == key).get.dataType match {
            case StringType =>
            case _ => throw new IllegalStateException(s"Ordering key column $key must be of StringType")
          }
        }

        PubsubWriter().write(data.queryExecution, pubsubWriteOptions, batchId)

        latestBatchId = batchId
      }
    }

  }
}

package org.apache.spark.sql.connector.pubsub

import com.google.api.core.{ApiFuture, ApiFutures}
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.types.StringType

import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.mutable

case class PubsubWriter() extends Logging {

  import PubsubWriter._

  def write(queryExecution: QueryExecution,
            pubsubWriteOptions: PubsubWriteOptions,
            batchId: Long): Unit = {

    SQLExecution.withNewExecutionId(queryExecution) {

      val attributes = queryExecution.analyzed.output

      queryExecution.toRdd.foreachPartition((dataPartition: Iterator[InternalRow]) => {

        val dataExpr = attributes.find(_.name == "data")
          .getOrElse(throw new IllegalStateException("data column not found in schema"))
        val attributesExpr = attributes.find(_.name == "attributes")
          .getOrElse(throw new IllegalStateException("attributes column not found in schema"))

        val keyExpr = pubsubWriteOptions.orderingKey.map { key =>
          val expr = attributes
            .find(_.name == key)
            .getOrElse(throw new IllegalStateException(s"Ordering key column $key not found in schema"))
          expr.dataType match {
            case StringType =>
            case _ => throw new IllegalStateException(s"Ordering key column $key must be of StringType")
          }
          expr
        }

        val projection = UnsafeProjection.create(Seq(dataExpr, attributesExpr) ++ keyExpr, attributes)

        val messageIdFutures = mutable.ListBuffer[ApiFuture[String]]()

        var publisher = CachedPublishers.getOrCreatePublisher(pubsubWriteOptions)

        dataPartition.foreach(row => {
          val unsafeRow = projection(row)
          val payload = ByteString.copyFrom(unsafeRow.getBinary(0))
          val attributes = unsafeMapDataToJavaMap(unsafeRow.getMap(1))
          val pubsubMessageBuilder = PubsubMessage.newBuilder
            .setData(payload)
            .putAllAttributes(attributes)

          if (pubsubWriteOptions.orderingKey.isDefined)
            pubsubMessageBuilder.setOrderingKey(unsafeRow.getString(3))

          @tailrec
          def publishMessage(pubsubMessage: PubsubMessage): Unit = {
            try {
              messageIdFutures += publisher.publish(pubsubMessage)
            } catch {
              // In case the publisher is closed, create a new one and publish the message
              case _: IllegalStateException =>
                logWarning("Publisher is shutdown. Creating a new one..")
                publisher = CachedPublishers.getOrCreatePublisher(pubsubWriteOptions, forceCreate = true)
                publishMessage(pubsubMessage)
            }
          }

          publishMessage(pubsubMessageBuilder.build())
        })

        try {
          val messageIds = ApiFutures
            .allAsList(messageIdFutures.asJava)
            .get(TimeoutSeconds, TimeUnit.SECONDS)
          logInfo(s"Published ${messageIds.size()} messages for BATCH: $batchId")
        } catch {
          case e: Exception =>
            logError("Error publishing messages", e)
            throw e
        }
      })
    }
  }

}

object PubsubWriter {
  private val TimeoutSeconds = sys.props
    .get("spark.sql.pubsub.writer.timeout.seconds")
    .fold(5 * 60) { value => value.toInt } // default to 5 minutes
}

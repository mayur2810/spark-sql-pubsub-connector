package org.apache.spark.sql.connector.pubsub

import com.google.api.gax.rpc.DeadlineExceededException
import com.google.pubsub.v1.{PullRequest, ReceivedMessage}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{SparkEnv, TaskContext}

import java.util
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}
import scala.collection.mutable


class PubsubPartitionReader(partition: PubsubInputPartition) extends PartitionReader[InternalRow] with Logging {

  private var receivedMessages: mutable.Seq[ReceivedMessage] = mutable.Seq.empty
  private var currentIndex = -1
  private var initialized = false
  private val blockManager = SparkEnv.get.blockManager

  // Register the partition's accumulator
  TaskContext.get().registerAccumulator(partition.ackIdAccumulator)

  /*
      Pulls data from Pub/sub and also stores it as an RDD block.
      This is to ensure that during multiple operations on the streaming batch, the reevaluation of
      the partition does not trigger a new pull from Pub/Sub.
    */
  private def pullData(): Unit = {
    blockManager.get(partition.rddBlockId) match {
      case Some(blockResult) =>
        logDebug(s"RDD block: ${partition.rddBlockId.name} found in BlockManager")
        receivedMessages = blockResult.data.asInstanceOf[Iterator[ReceivedMessage]].toBuffer
      case None =>
        logDebug(s"RDD block: ${partition.rddBlockId.name} not found in BlockManager. " +
          s"Pulling data from Pub/Sub subscription: ${partition.options.getSubscription}")

        val pr = PullRequest.newBuilder()
          .setMaxMessages(partition.options.maxMessagesPerPartition)
          .setSubscription(partition.options.getSubscription)
          .setReturnImmediately(true)
          // Note: Using this since timeout on the client causes recurrent
          // loss of buffered data, which is not good
          // Need to check how to better handle this situation on client side
          // until then setting this option to true.
          .build()

        try {
          val pullResponse = Subscriber.getOrCreate(partition.endpointOverride, partition.options).pullCallable().call(pr)
          receivedMessages = pullResponse.getReceivedMessagesList.asScala
          if (pullResponse.getReceivedMessagesCount > 0) {
            // TODO: As another level redundancy, can data be stored to the checkpoint location?
            if (blockManager.putIterator(partition.rddBlockId, receivedMessages.iterator, StorageLevel.MEMORY_AND_DISK_SER_2))
              logDebug(s"Stored RDD block: ${partition.rddBlockId.name}")
            else {
              logError(s"Failed to store RDD block: ${partition.rddBlockId.name}")
              throw new Exception(s"Failed to store RDD block: ${partition.rddBlockId.name}")
            }
          }
        } catch {
          case _: DeadlineExceededException =>
            logWarning(s"Pull request timed out for subscription" +
              s" ${partition.options.getSubscription}. Probably no messages available.")
        }
    }
  }


  override def next(): Boolean = {
    if (!initialized) {
      pullData()
      initialized = true
    }
    currentIndex += 1
    currentIndex < receivedMessages.size
  }

  override def get(): InternalRow = {
    val receivedMessage = receivedMessages(currentIndex)
    val message = receivedMessage.getMessage
    val attributeMap = message.getAttributesMap.asScala
    InternalRow.fromSeq(Seq(
      UTF8String.fromString(partition.options.getSubscription),
      UTF8String.fromString(receivedMessage.getAckId),
      UTF8String.fromString(message.getMessageId),
      UTF8String.fromString(message.getOrderingKey),
      message.getData.toByteArray,
      message.getPublishTime.getSeconds * 1000 * 1000 + message.getPublishTime.getNanos / 1000,
      new ArrayBasedMapData(
        ArrayData.toArrayData(attributeMap.keys.toArray.map(UTF8String.fromString)),
        ArrayData.toArrayData(attributeMap.values.toArray.map(UTF8String.fromString))
      )))
  }

  // Saves the acknowledgment ids to the partition's accumulator
  override def close(): Unit = {
    if (receivedMessages.nonEmpty)
      partition.ackIdAccumulator.setValue(util.Arrays.asList(receivedMessages.map(_.getAckId): _*))
  }
}

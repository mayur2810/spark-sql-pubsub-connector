package org.apache.spark.sql.connector.pubsub

import com.google.common.base.Stopwatch
import com.google.pubsub.v1.AcknowledgeRequest
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit, SupportsTriggerAvailableNow}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.RDDBlockId

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter}
import scala.collection.mutable

case class PubsubMicroBatchStream(schema: StructType, options: PubsubReadOptions, checkpointLocation: String)
  extends Logging with MicroBatchStream with SupportsTriggerAvailableNow {

  import PubsubMicroBatchStream._

  private val sc = SparkSession.active.sparkContext
  private var processedOffsets = new LongOffset(0L)
  private val ackIdAccumulator = sc.collectionAccumulator[String]
  private val subscriber = Subscriber.getOrCreate(None, options)
  private var availableNowMaxBatches = -1

  registerStream(options.getSubscription, checkpointLocation)

  if (options.dynamicPartitioning)
    PubsubSubscriptionMonitor.registerSubscription(options.pubsubProjectId, options.pubsubSubscription)

  // There is no concept of offset in Pub/Sub.
  // We use a simple counter to keep track of the batches processed.
  override def latestOffset(): Offset = {
    processedOffsets = processedOffsets + 1
    processedOffsets
  }

  /*
     We create and attach an RDD Block to each partition to store the data fetched from Pub/Sub during partition read.
     This is to ensure that during multiple operations on the stream batch, the reevaluation of
     the source does not trigger a new pull from Pub/Sub.
   */
  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val endOffset = end.asInstanceOf[LongOffset].offset
    if (endOffset > 0)
      processedOffsets = end.asInstanceOf[LongOffset]
    val rddId = getRDDId(options.getSubscription, endOffset)

    val partitioningInfo =
      if (options.dynamicPartitioning)
        PubsubSubscriptionMonitor
          .getNumPartitionsWithRegionSplits(options.pubsubProjectId, options.pubsubSubscription)
      else
        PartitioningInfo(options.numPartitions, Nil)

    if (partitioningInfo.splitRegionWise) {
      val regionSplits = partitioningInfo.regions
      logDebug(s"Region-wise partitioning is enabled for Subscription ${options.getSubscription}: $regionSplits")
      regionSplits
        .foldLeft(Nil: Seq[PubsubInputPartition]) {
        case (inputPartitions, partitioningInfoForRegion) =>
          val region = partitioningInfoForRegion.region
          val regionSpecificPartitions = Math.max(
            partitioningInfoForRegion.numPartitions,
            options.numPartitions / regionSplits.size)
          logInfo(s"$regionSpecificPartitions input partitions planned for Subscription: " +
            s"${options.getSubscriptionName}, Region: ${region.toString} with RDD Id: ${rddId.toLong}")
          inputPartitions ++ (0 until regionSpecificPartitions).map { index: Int =>
            PubsubInputPartition(RDDBlockId(rddId, inputPartitions.size + index),
              options, ackIdAccumulator, Some(region.getRegionEndpoint))
          }
      }.toArray
    } else {
      val partitions = Math.max(partitioningInfo.totalPartitions, options.numPartitions)
      logInfo(s"$partitions input partitions planned for Subscription: " +
        s"${options.getSubscriptionName} with RDD Id: ${rddId.toLong}")
      (0 until partitions).map { index: Int =>
        PubsubInputPartition(RDDBlockId(rddId, index), options, ackIdAccumulator)
      }.toArray
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = new PubsubPartitionReaderFactory()

  override def initialOffset(): Offset = new LongOffset(0L)

  override def deserializeOffset(json: String): Offset = LongOffset(json.toLong)

  // Collects Ack Ids from each partition (to driver) and commits them to Pub/Sub
  // Also, evicts the RDD from BlockManager
  override def commit(end: Offset): Unit = {
    val ackIds = ackIdAccumulator.value.asScala.distinct
    val stopwatch = Stopwatch.createStarted()
    ackIds
      .grouped(1500)
      .toParArray
      .foreach { ackIds =>
        val acknowledgeRequest =
          AcknowledgeRequest.newBuilder()
            .setSubscription(options.getSubscription)
            .addAllAckIds(ackIds.asJava)
            .build()
        subscriber.acknowledgeCallable().call(acknowledgeRequest)
      }
    ackIdAccumulator.reset()
    val rddId = getRDDId(options.getSubscription, end.asInstanceOf[LongOffset].offset)
    sc.env.blockManager.master.removeRdd(rddId, blocking = true)
    removeRDDId(options.getSubscription, end.asInstanceOf[LongOffset].offset)
    stopwatch.stop()
    logInfo(s"Committed ${ackIds.size} Ack ids for Subscription: " +
      s"${options.getSubscriptionName} in ${stopwatch.elapsed(TimeUnit.SECONDS)} seconds")
  }

  override def stop(): Unit = {
    subscriber.shutdown()
    getAllRDDIds(options.getSubscription).foreach { rddId =>
      sc.env.blockManager.master.removeRdd(rddId, blocking = true)
    }
    removeRDDId(options.getSubscription, -1)
    deregisterStream(options.getSubscription)
    PubsubSubscriptionMonitor.deregisterSubscription(options.pubsubProjectId, options.pubsubSubscription)
  }

  override def prepareForTriggerAvailableNow(): Unit = {
    if (!options.dynamicPartitioning)
      PubsubSubscriptionMonitor
        .registerSubscription(options.pubsubProjectId, options.pubsubSubscription)
    availableNowMaxBatches = PubsubSubscriptionMonitor
      .getNumberOfBatches(options.pubsubProjectId, options.pubsubSubscription)
  }

  override def latestOffset(startOffset: Offset, limit: ReadLimit): Offset = {
    val endOffset = startOffset.asInstanceOf[LongOffset].offset
    if (availableNowMaxBatches != -1 && endOffset > availableNowMaxBatches)
      logInfo("All available batches have been processed.")
    else
      processedOffsets = processedOffsets + 1
    processedOffsets
  }
}

object PubsubMicroBatchStream {

  private val streamsMap = mutable.Map[String, String]()
  private val rddMap = mutable.Map[(String, Long), Int]()

  private def getRDDId(subscription: String, endOffset: Long): Int = this.synchronized {
    rddMap.getOrElseUpdate((subscription, endOffset), SparkSession.active.sparkContext.newRddId())
  }

  private def getAllRDDIds(subscription: String): Seq[Int] = this.synchronized {
    rddMap.filterKeys(_._1 == subscription).values.toSeq
  }

  private def removeRDDId(subscription: String, endOffset: Long): Unit = this.synchronized {
    endOffset match {
      case -1 =>
        rddMap.filterKeys(_._1 == subscription).foreach { case (key, _) => rddMap.remove(key) }
      case _ =>
        rddMap.remove((subscription, endOffset))
    }
  }

  private def deregisterStream(subscription: String): Unit = this.synchronized {
    streamsMap.remove(subscription)
  }

  /*
     Scenarios where same subscription is used in multiple streams
     and/or split into multiple streams results in an error.
   */
  private def registerStream(subscription: String, checkpointLocation: String) = this.synchronized {
    if (streamsMap.contains(subscription) && streamsMap(subscription) != checkpointLocation)
      throw new IllegalStateException(s"Found multiple streams reading the same Pub/Sub " +
        s"subscription: ${subscription}! Please use different subscriptions.")
    else if (!streamsMap.contains(subscription))
      streamsMap.put(subscription, checkpointLocation)
  }

}

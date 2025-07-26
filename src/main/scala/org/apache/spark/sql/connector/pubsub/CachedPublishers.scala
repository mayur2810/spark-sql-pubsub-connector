package org.apache.spark.sql.connector.pubsub

import com.google.api.gax.batching.FlowController.LimitExceededBehavior
import com.google.api.gax.batching.{BatchingSettings, FlowControlSettings}
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.Publisher
import io.grpc.ManagedChannelBuilder
import org.apache.spark.util.ShutdownHookManager
import org.threeten.bp.Duration

import scala.collection.mutable

object CachedPublishers {

  private val publishers = mutable.Map.empty[PubsubWriteOptions, Publisher]

  private def defaultPublisherSettings(topic: String): Publisher.Builder = {
    val flowControlSettings =
      FlowControlSettings.newBuilder()
        .setLimitExceededBehavior(LimitExceededBehavior.Block)
        .setMaxOutstandingRequestBytes(20 * 1024 * 1024L)
        .setMaxOutstandingElementCount(1000L)
        .build()

    val batchingSettings = BatchingSettings.newBuilder()
      .setFlowControlSettings(flowControlSettings)
      .setElementCountThreshold(20)
      .setDelayThreshold(Duration.ofMillis(10))
      .build()

    Publisher.newBuilder(topic)
      .setBatchingSettings(batchingSettings)
  }

  // for testing purposes
  private def customPublisherSettings(topic: String, endpoint: String): Publisher.Builder = {
    Publisher.newBuilder(topic)
      .setChannelProvider(
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(ManagedChannelBuilder
          .forTarget(endpoint)
          .usePlaintext()
          .asInstanceOf[ManagedChannelBuilder[_]]
          .build())))
      .setCredentialsProvider(NoCredentialsProvider.create())
  }

  private def createPublisher(options: PubsubWriteOptions): Publisher = {
    options.endPoint.fold(defaultPublisherSettings(options.getTopic)) { endpoint =>
        customPublisherSettings(options.getTopic, endpoint)
      }
      .setEnableMessageOrdering(options.orderingKey.isDefined)
      .build
  }


  def getOrCreatePublisher(options: PubsubWriteOptions, forceCreate: Boolean = false): Publisher = this.synchronized {
    if (forceCreate)
      publishers.update(options, createPublisher(options))
    publishers.getOrElseUpdate(options, createPublisher(options))
  }

  // Register for shutdown on termination
  ShutdownHookManager.addShutdownHook(() => publishers.values.foreach(_.shutdown()))
}

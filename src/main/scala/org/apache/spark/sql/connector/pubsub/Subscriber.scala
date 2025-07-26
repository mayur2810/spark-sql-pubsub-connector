package org.apache.spark.sql.connector.pubsub

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.{ChannelPoolSettings, GrpcTransportChannel}
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import io.grpc.ManagedChannelBuilder
import org.apache.spark.util.ShutdownHookManager
import org.threeten.bp.Duration

import scala.collection.mutable

object Subscriber {

  private val PullTimeOutDefault = Duration.ofSeconds(10)
  private val GlobalEndpoint = "pubsub.googleapis.com:443"
  private val LocalEndpointHost = "localhost"

  private def defaultSubscriberStubSettings(endpoint: String) = {
    val builder = SubscriberStubSettings.newBuilder()
      .setTransportChannelProvider(
        SubscriberStubSettings
          .defaultGrpcTransportProviderBuilder()
          .setMaxInboundMessageSize(20 * 1024 * 1024)
          .setEndpoint(endpoint)
          .setChannelPoolSettings(ChannelPoolSettings.builder()
            .setInitialChannelCount(4)
            .setMinChannelCount(4)
            .build())
          .build())
    builder
      .pullSettings()
      .setSimpleTimeoutNoRetries(PullTimeOutDefault)
      .build()
    builder.build()
  }

  // for testing purposes
  private def customSubscriberSettings(endpoint: String) = {
    val builder = SubscriberStubSettings.newBuilder()
      .setTransportChannelProvider(
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(ManagedChannelBuilder
          .forTarget(endpoint)
          .usePlaintext()
          .asInstanceOf[ManagedChannelBuilder[_]]
          .build())))
    builder
      .pullSettings()
      .setSimpleTimeoutNoRetries(PullTimeOutDefault)
      .build()
    builder
      .setCredentialsProvider(NoCredentialsProvider.create())
      .build()
  }


  private val subscribers: mutable.Map[String, GrpcSubscriberStub] = mutable.Map.empty

  private def initSubscriber(endpoint:String, settings: SubscriberStubSettings): Unit = this.synchronized {
    if (!subscribers.contains(endpoint) || subscribers(endpoint).isTerminated)
      subscribers.put(endpoint, GrpcSubscriberStub.create(settings))
  }

  def getOrCreate(endpointOverride: Option[String], pubsubReadOptions: PubsubReadOptions): GrpcSubscriberStub = {
    val endpoint = endpointOverride.getOrElse{
      pubsubReadOptions.endPoint match {
        case Some(endpoint) => endpoint
        case None => GlobalEndpoint
      }
    }.toLowerCase()

    if (!subscribers.contains(endpoint) || subscribers(endpoint).isTerminated) {
      val settings = if (endpoint.startsWith(LocalEndpointHost))
        customSubscriberSettings(endpoint)
      else
        defaultSubscriberStubSettings(endpoint)
      initSubscriber(endpoint, settings)
    }
    subscribers(endpoint)
  }

  // Register for shutdown on termination
  ShutdownHookManager.addShutdownHook(() => subscribers.foreach(_._2.shutdown()))
}

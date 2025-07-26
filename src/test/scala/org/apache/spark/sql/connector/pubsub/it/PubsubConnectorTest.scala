package org.apache.spark.sql.connector.pubsub.it

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.google.cloud.pubsub.v1.{SubscriptionAdminClient, TopicAdminClient, TopicAdminSettings}
import com.google.protobuf.ByteString
import com.google.pubsub.v1._
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsJavaMapConverter, seqAsJavaListConverter}

class PubsubConnectorTest extends AnyFunSuite
  with Matchers
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with Logging {

  private val streamsTerminationTimeout = 10 * 1000
  private val hostPort = "localhost:8085"
  private val testProject = "test-project"

  private val sourceTopicName = "source-topic"
  private val sourceSubscriptionName = "source-subscription"
  private val sourceTopic: TopicName = TopicName.of(testProject, sourceTopicName)
  private val sourceSubscription: SubscriptionName = SubscriptionName.of(testProject, sourceSubscriptionName)


  private val sinkTopicName = "sink-topic"
  private val sinkSubscriptionName = "sink-subscription"
  private val sinkTopic: TopicName = TopicName.of(testProject, sinkTopicName)
  private val sinkSubscription: SubscriptionName = SubscriptionName.of(testProject, sinkSubscriptionName)


  private var topicClient: TopicAdminClient = _
  private var subscriptionClient: SubscriptionAdminClient = _
  private val channel: ManagedChannel = ManagedChannelBuilder
    .forTarget(hostPort)
    .usePlaintext().build()


  private def publishDataToSourceTopic(numMessages: Int): Unit = {

    val data = List.range(0, numMessages).map(i => {
      PubsubMessage.newBuilder()
        .setData(ByteString.copyFromUtf8(s"Test Message: $i"))
        .putAllAttributes(Map("key" -> s"value: $i").asJava)
        .build()
    })

    val response = topicClient.publish(sourceTopic, data.asJava)
    logInfo(s"Published ${response.getMessageIdsCount} messages to topic: $sourceTopicName")
  }

  private def createTopic(topic: TopicName): Unit = {
    try {
      topicClient.getTopic(topic)
      topicClient.deleteTopic(topic)
      topicClient.createTopic(topic)
      logInfo(s"Created topic: $topic")
    } catch {
      case _: Exception =>
        topicClient.createTopic(topic)
        logInfo(s"Created topic: $topic")
    }
  }

  private def createSubscription(subscription: SubscriptionName, topic: TopicName): Unit = {
    try {
      subscriptionClient.getSubscription(subscription)
      subscriptionClient.deleteSubscription(subscription)
      subscriptionClient.createSubscription(subscription, topic, PushConfig.getDefaultInstance, 600)
      logInfo(s"Created subscription: $subscription")
    } catch {
      case _: Exception =>
        subscriptionClient.createSubscription(subscription, topic, PushConfig.getDefaultInstance, 600)
        logInfo(s"Created subscription: $subscription")
    }
  }

  override def beforeEach(): Unit = {
    logInfo("Initializing Pubsub Environment")
    // Init topics and subscriptions
    val channelProvider =
      FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
    val credentialsProvider = NoCredentialsProvider.create()

    topicClient =
      TopicAdminClient.create(
        TopicAdminSettings.newBuilder()
          .setTransportChannelProvider(channelProvider)
          .setCredentialsProvider(credentialsProvider)
          .build())

    subscriptionClient = SubscriptionAdminClient
      .create(GrpcSubscriberStub.create(SubscriberStubSettings.newBuilder()
        .setTransportChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider)
        .build()))

    createTopic(sourceTopic)
    createSubscription(sourceSubscription, sourceTopic)

    createTopic(sinkTopic)
    createSubscription(sinkSubscription, sinkTopic)

  }


  test("Pubsub Source Test", IntegrationTestTag) {

    publishDataToSourceTopic(100)

    val spark = SparkSession
      .builder()
      .appName("PubsubSourceTest")
      .master("local[*]")
      .getOrCreate()

    val pubsubdata = spark.readStream
      .option("subscription", sourceSubscriptionName)
      .option("project_id", testProject)
      .option("max_messages_per_partition", 10)
      .option("endpoint", hostPort)
      .option("dynamic_partitioning", "false")
      .format("pubsub")
      .load()

    val countAccumulator = spark.sparkContext.longAccumulator

    val query = pubsubdata
      .writeStream
      .option("checkpointLocation", "./checkpoints/pubsub_sink_test/test1")
      .foreachBatch((ds: Dataset[Row], batchId: Long) => {
        ds.show(false)
        logInfo(s"Batch count: ${ds.count()}")
        countAccumulator.add(ds.count())
      })
      .start()

    spark.streams.awaitAnyTermination(streamsTerminationTimeout)
    query.stop()
    spark.stop()
    logInfo("Spark Session Stopped")
    assert(countAccumulator.value == 100)
  }

  test("Pubsub Sink Test", IntegrationTestTag) {

    publishDataToSourceTopic(100)

    val spark = SparkSession
      .builder()
      .appName("PubsubSinkTest")
      .master("local[*]")
      .getOrCreate()

    val pubsubdata = spark.readStream
      .option("subscription", sourceSubscriptionName)
      .option("project_id", testProject)
      .option("max_messages_per_partition", 10)
      .option("dynamic_partitioning", "false")
      .option("endpoint", hostPort)
      .format("pubsub")
      .load()

    val query = pubsubdata
      .writeStream
      .format("pubsub")
      .option("topic", sinkTopicName)
      .option("project_id", testProject)
      .option("endpoint", hostPort)
      .option("checkpointLocation", "./checkpoints/pubsub_sink_test/test2")
      .start()

    spark.streams.awaitAnyTermination(streamsTerminationTimeout)
    query.stop()
    spark.stop()

    val pr = PullRequest.newBuilder()
      .setMaxMessages(1000)
      .setSubscription(sinkSubscription.toString)
      .build()

    val sinkTopicMessages = subscriptionClient.pullCallable().call(pr).getReceivedMessagesList
    assert(sinkTopicMessages.size() == 100)

    val message = sinkTopicMessages.asScala.last
    assert(message.getMessage.getData.toStringUtf8.startsWith("Test Message"))
    assert(message.getMessage.getAttributesMap.get("key").startsWith("value:"))
  }


  test("Splits of the pubsub input stream using same subscription should fail", IntegrationTestTag) {

    val spark = SparkSession
      .builder()
      .appName("PubsubSplitSourceTest")
      .master("local[*]")
      .getOrCreate()

    val pubsubData = spark.readStream
      .option("subscription", sourceSubscriptionName)
      .option("project_id", testProject)
      .option("max_messages_per_partition", 20)
      .option("endpoint", hostPort)
      .option("dynamic_partitioning", "false")
      .format("pubsub")
      .load()

    val splitStream1 = pubsubData.filter("true = true")
    val splitStream2 = pubsubData.filter("true = true")

    val caught = intercept[StreamingQueryException] {
      splitStream1
        .writeStream
        .format("pubsub")
        .option("topic", sinkTopicName)
        .option("project_id", testProject)
        .option("endpoint", hostPort)
        .option("dynamic_partitioning", "false")
        .option("checkpointLocation", "./checkpoints/pubsub_sink_test/test3-1")
        .start()

      splitStream2
        .writeStream
        .format("pubsub")
        .option("topic", sinkTopicName)
        .option("project_id", testProject)
        .option("endpoint", hostPort)
        .option("dynamic_partitioning", "false")
        .option("checkpointLocation", "./checkpoints/pubsub_sink_test/test3-2")
        .start()

      spark.streams.awaitAnyTermination(streamsTerminationTimeout)
    }
    assert(caught.getCause.isInstanceOf[IllegalStateException])
    spark.stop()
  }


  test("Multiple input streams reading the same subscription should fail", IntegrationTestTag) {

    val spark = SparkSession
      .builder()
      .appName("PubsubMultiInputSourceTest")
      .master("local[*]")
      .getOrCreate()

    val pubsubData1 = spark.readStream
      .option("subscription", sourceSubscriptionName)
      .option("project_id", testProject)
      .option("max_messages_per_partition", 20)
      .option("endpoint", hostPort)
      .option("dynamic_partitioning", "false")
      .format("pubsub")
      .load()

    val pubsubData2 = spark.readStream
      .option("subscription", sourceSubscriptionName)
      .option("project_id", testProject)
      .option("max_messages_per_partition", 20)
      .option("endpoint", hostPort)
      .option("dynamic_partitioning", "false")
      .format("pubsub")
      .load()

    val merged = pubsubData1.unionAll(pubsubData2)

    val caught = intercept[StreamingQueryException] {
      merged
        .writeStream
        .format("pubsub")
        .option("topic", sinkTopicName)
        .option("project_id", testProject)
        .option("endpoint", "localhost:8085")
        .option("checkpointLocation", "./checkpoints/pubsub_sink_test/test4")
        .start()

      spark.streams.awaitAnyTermination(streamsTerminationTimeout)
    }
    assert(caught.getCause.isInstanceOf[IllegalStateException])
    spark.stop()
  }

  override def afterAll(): Unit = {
    channel.shutdown()
  }

}

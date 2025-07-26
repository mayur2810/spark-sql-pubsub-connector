package org.apache.spark.sql.connector

import org.apache.spark.sql.catalyst.expressions.UnsafeMapData
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

package object pubsub {

  // Option key for the Pub/Sub subscription.
  val SubscriptionOption = "subscription"

  // Option key for the Google Cloud project ID.
  val ProjectIdOption = "project_id"

  // Option key for the Pub/Sub topic.
  val TopicOption = "topic"

  // Option key for the Pub/Sub endpoint (for testing purposes).
  val EndPointOption = "endpoint"

  // Option key for the ordering key.
  val OrderingKeyOption = "ordering_key"

  // Option key for the maximum number of messages per partition.
  val MaxMessagesPerPartitionOption = "max_messages_per_partition"

  // Option key for dynamic partitioning.
  val DynamicPartitioningOption = "dynamic_partitioning"

  // Option key for the number of partitions.
  val NumberOfPartitionsOption = "num_partitions"

  // Default value for the maximum number of messages per partition.
  val DefaultMaxMessagesPerPartition = 1000

  // Default value for the number of partitions.
  val DefaultNumberOfPartitions = 4

  val DefaultRegion = "global"

  /**
   * Case class representing the options for reading from Pub/Sub.
   *
   * @param pubsubSubscription      The Pub/Sub subscription.
   * @param pubsubProjectId         The Google Cloud project ID.
   * @param endPoint                The optional Pub/Sub endpoint e.g. localhost:8085 (for testing purposes)
   * @param numPartitions           The number of partitions.
   * @param maxMessagesPerPartition The maximum number of messages per partition.
   */
  case class PubsubReadOptions(pubsubSubscription: String,
                               pubsubProjectId: String,
                               endPoint: Option[String],
                               numPartitions: Int,
                               maxMessagesPerPartition: Int,
                               dynamicPartitioning: Boolean = true) {
    def getSubscription = s"projects/$pubsubProjectId/subscriptions/$pubsubSubscription"
    def getSubscriptionName = s"$pubsubProjectId:$pubsubSubscription"
  }

  /**
   * Case class representing the options for writing to Pub/Sub.
   *
   * @param pubsubProjectId The Google Cloud project ID.
   * @param pubsubTopic     The Pub/Sub topic.
   * @param orderingKey     The optional ordering key.
   * @param endPoint        The optional Pub/Sub endpoint e.g. localhost:8085 (for testing purposes)
   */
  case class PubsubWriteOptions(pubsubProjectId: String,
                                pubsubTopic: String,
                                orderingKey: Option[String],
                                endPoint: Option[String]) {
    def this(fullQualifiedTopicPath: String, orderingKey: Option[String], endPoint: Option[String]) =
      this(
        fullQualifiedTopicPath.split("/")(1).trim,
        fullQualifiedTopicPath.split("/")(3).trim,
        orderingKey,
        endPoint)


    def getTopic = s"projects/$pubsubProjectId/topics/$pubsubTopic"
  }

  /**
   * Class representing a GCP region.
   *
   * @param region The region string.
   */
  case class Region(region: String) {

    def this() = this(DefaultRegion)

    def getRegionEndpoint: String = region.trim.toLowerCase match {
      case DefaultRegion => s"pubsub.googleapis.com:443"
      case _ => s"$region-pubsub.googleapis.com:443"
    }

    override def toString: String = region
  }

  /**
   * Case class representing partitioning information for a specific region.
   *
   * @param region               The region.
   * @param numPartitions        The number of partitions.
   * @param numUndeliveredMessages The number of undelivered messages.
   */
  case class PartitioningInfoForRegion(region: Region,
                                       numPartitions: Int,
                                       numUndeliveredMessages: Long)


  /**
   * Case class representing partitioning information for a Pub/Sub subscription.
   * @param totalPartitions       The total number of partitions.
   * @param regions  The list of partitioning information for each region.
   */
  case class PartitioningInfo(totalPartitions: Int,
                              regions: List[PartitioningInfoForRegion],
                              regionWiseSplitThreshold: Double = 0.7) {

    private val totalUndeliveredMessages: Long = regions.map(_.numUndeliveredMessages).sum

    private val regionWiseFractions: List[(Region, Double)] = regions.map { p =>
      (p.region, p.numUndeliveredMessages.toDouble / totalUndeliveredMessages)
    }

    def splitRegionWise: Boolean = {
      regions.size > 1 && regionWiseFractions.exists(_._2 > regionWiseSplitThreshold)
    }
  }

  /**
   * Validates and initializes the read options from a map of options.
   *
   * @param options The map of options.
   * @return The validated and initialized PubsubReadOptions.
   * @throws IllegalArgumentException if required options are missing or invalid.
   */
  def validateAndInitReadOptions(options: Map[String, String]): PubsubReadOptions = {
    val subscription = options.getOrElse(SubscriptionOption,
      throw new IllegalArgumentException(s"${SubscriptionOption} must be provided"))
    val projectId = options.getOrElse(ProjectIdOption,
      throw new IllegalArgumentException(s"${ProjectIdOption} must be provided"))
    val numPartitions = options.get(NumberOfPartitionsOption).map(_.toInt).getOrElse(DefaultNumberOfPartitions)
    val maxMessagesPerPartition = options.get(MaxMessagesPerPartitionOption).map(_.toInt).getOrElse(DefaultMaxMessagesPerPartition)
    val dynamicPartitioning = options.get(DynamicPartitioningOption).map(_.toBoolean).getOrElse(true)

    if (maxMessagesPerPartition < 1)
      throw new IllegalArgumentException("max_messages_per_partition must be greater than 0")

    if (numPartitions < 1)
      throw new IllegalArgumentException("num_partitions must be greater than 0")

    PubsubReadOptions(subscription, projectId, options.get(EndPointOption), numPartitions,
      maxMessagesPerPartition, dynamicPartitioning)
  }

  /**
   * Validates and initializes the write options from a map of options.
   *
   * @param options The map of options.
   * @return The validated and initialized PubsubWriteOptions.
   * @throws IllegalArgumentException if required options are missing.
   */
  def validateAndInitWriteOption(options: Map[String, String]): PubsubWriteOptions = {
    val projectId = options.getOrElse(ProjectIdOption,
      throw new IllegalArgumentException(s"${ProjectIdOption} must be provided"))
    val topic = options.getOrElse(TopicOption,
      throw new IllegalArgumentException(s"${TopicOption} must be provided"))

    PubsubWriteOptions(projectId, topic, options.get(OrderingKeyOption), options.get(EndPointOption))
  }

  // Schema for reading from Pub/Sub.
  val PubsubReadSchema = new StructType(
    Array[StructField](
      StructField("subscription", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("ack_id", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("message_id", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("ordering_key", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("data", DataTypes.BinaryType, nullable = false, Metadata.empty),
      StructField("publish_timestamp", DataTypes.TimestampType, nullable = false, Metadata.empty),
      StructField("attributes",
        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
        nullable = true,
        Metadata.empty))
  )

  // Schema for writing to Pub/Sub.
  val PubsubWriteSchema = new StructType(
    Array[StructField](
      StructField("data", DataTypes.BinaryType, nullable = false, Metadata.empty),
      StructField("attributes",
        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType),
        nullable = true,
        Metadata.empty))
  )

  /**
   * Converts an UnsafeMapData instance to a Java Map.
   *
   * @param unsafeMapData The UnsafeMapData instance to convert.
   * @return A Java Map containing the same data as the UnsafeMapData instance.
   */
  def unsafeMapDataToJavaMap(unsafeMapData: UnsafeMapData): java.util.Map[String, String] = {

    val numElements = unsafeMapData.numElements()
    val javaMap = new java.util.HashMap[String, String]()

    val keys = unsafeMapData.keyArray()
    val values = unsafeMapData.valueArray()

    for (i <- 0 until numElements) {
      val key = keys.getUTF8String(i).toString
      val value = values.getUTF8String(i).toString
      javaMap.put(key, value)
    }

    javaMap
  }

}

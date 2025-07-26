package org.apache.spark.sql.connector.pubsub

import com.google.cloud.monitoring.v3.MetricServiceClient
import com.google.monitoring.v3.{Aggregation, ListTimeSeriesRequest, TimeInterval}
import com.google.protobuf.Timestamp
import org.apache.spark.internal.Logging
import org.apache.spark.util.ShutdownHookManager

import java.util.Timer
import scala.collection.mutable
import scala.collection.JavaConverters._

/*
    Monitors the number of undelivered messages in a Pub/Sub subscription and
    calculates the number of partitions to be used for reading the subscription.
 */
object PubsubSubscriptionMonitor extends Logging {

  val PubsubMaxDynamicPartitionsConfig = "spark.sql.pubsub.max.dynamic.partitions"
  val PubsubMetricHostProjectConfig = "spark.sql.pubsub.metric.host.project"
  val PubsubMonitoringIntervalConfig = "spark.sql.pubsub.monitoring.interval"
  val PubsubBacklogThresholdConfig = "spark.sql.pubsub.backlog.threshold"
  val PubsubRegionWiseSplitThresholdConfig = "spark.sql.pubsub.region.split.threshold"

  private val props = sys.props
  private val MinPartitions = 1
  private val DefaultNumRecords = 1000L

  private val MaxPartitions = props
    .get(PubsubMaxDynamicPartitionsConfig)
    .fold(256)(value => Math.max(value.toInt, 32))

  private val BacklogThreshold = props
    .get(PubsubBacklogThresholdConfig)
    .fold(10000)(value => Math.max(value.toInt, 1000))

  private val PubsubMonitoringInterval = props
    .get(PubsubMonitoringIntervalConfig)
    .fold(10 * 60 * 1000)(value => Math.max(value.toInt, 30 * 1000))

  private val RegionWiseSplitThreshold = props
    .get(PubsubRegionWiseSplitThresholdConfig)
    .fold(0.7)(value => Math.max(value.toDouble, 0.5))


  private val MetricHostProject = props.get(PubsubMetricHostProjectConfig)

  private val subscriptions: mutable.Map[(String, String), List[PartitioningInfoForRegion]] = mutable.Map.empty
  private val metricServiceClient = MetricServiceClient.create()

  def registerSubscription(projectId: String, subscription: String): Unit = {
    subscriptions.put((projectId, subscription), calculatePartitionsByRegion(projectId, subscription))
  }

  def deregisterSubscription(projectId: String, subscription: String): Unit = {
    subscriptions.remove((projectId, subscription))
  }

  private def updatePartitions(): Unit = {
    subscriptions.foreach { case ((projectId, subscription), _) =>
      subscriptions.put((projectId, subscription), calculatePartitionsByRegion(projectId, subscription))
    }
  }

  /**
   * Returns the number of partitions to be used for reading the subscription.
   */
  def getNumPartitions(projectId: String, subscription: String): Int = {
    subscriptions.getOrElse((projectId, subscription), List.empty) match {
      case Nil =>
        logWarning(s"No partitioning info found for $projectId:$subscription, defaulting to $MinPartitions")
        MinPartitions
      case partitionInfo =>
        val numPartitions = partitionInfo.map(_.numPartitions).sum
        logDebug(s"Number of partitions for $projectId:$subscription: $numPartitions")
        numPartitions
    }
  }

  def getNumPartitionsWithRegionSplits(projectId: String, subscription: String): PartitioningInfo = {
    subscriptions.getOrElse((projectId, subscription), List.empty) match {
      case Nil =>
        logWarning(s"No partitioning info found for $projectId:$subscription, defaulting to $MinPartitions")
        PartitioningInfo(MinPartitions, List.empty, RegionWiseSplitThreshold)
      case partitionList =>
        val numPartitions = Math.min(partitionList.map(_.numPartitions).sum, MaxPartitions)
        logDebug(s"Number of partitions for $projectId:$subscription: $numPartitions")
        PartitioningInfo(numPartitions, partitionList, RegionWiseSplitThreshold)
    }
  }

  /**
   * Returns the estimate of the number of batches to completely read the data in the pub/sub topic.
   * Used when the trigger is set to `available_now`.
   */
  def getNumberOfBatches(projectId: String, subscription: String): Int = {
    val (partitions, numUndeliveredMessages) = subscriptions
      .getOrElse((projectId, subscription), List.empty) match {
      case Nil =>
        (MinPartitions, DefaultNumRecords)
      case partitionInfo =>
        val numUndeliveredMessages = partitionInfo.map(_.numUndeliveredMessages).sum
        val partitions = partitionInfo.map(_.numPartitions).sum
        logInfo(s"Number of undelivered messages for $projectId:$subscription: $numUndeliveredMessages")
        logInfo(s"Number of partitions for $projectId:$subscription: $partitions")
        (partitions, numUndeliveredMessages)
    }
    // A maximum of 1000 records can be fetched in a single pull request. We consider 500 records
    // since not all 1000 records may not be returned even if available.
    val numberOfBatches = Math.max(numUndeliveredMessages / (partitions * 500), 1)
    logInfo(s"Number of batches for $projectId:$subscription: $numberOfBatches")
    numberOfBatches.toInt
  }

  private def calculatePartitions(projectId: String,
                                  subscription: String): (Int, Long) = {
    val request = ListTimeSeriesRequest
      .newBuilder()
      .setName(s"projects/${MetricHostProject.getOrElse(projectId)}")
      .setFilter(
        s""" metric.type="pubsub.googleapis.com/subscription/num_undelivered_messages"
           |AND resource.labels.subscription_id="$subscription"
           |AND resource.label.project_id="$projectId" """.stripMargin)
      .setInterval(TimeInterval.newBuilder()
        .setEndTime(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000))
        .setStartTime(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000 - 600))
        .build())
      .setView(ListTimeSeriesRequest.TimeSeriesView.FULL)
      .build()

    try {
      val res = metricServiceClient.listTimeSeries(request)
      val it = res.getPage.getValues.iterator()
      if (it.hasNext) {
        val numUndeliveredMessages = it.next().getPoints(0).getValue.getInt64Value
        val partitions = Math.max(
          Math.min(
            Math.ceil(numUndeliveredMessages / BacklogThreshold).toInt,
            MaxPartitions),
          MinPartitions
        )
        logInfo(s"Number of undelivered messages in $projectId:$subscription: $numUndeliveredMessages")
        logInfo(s"Partition count for $projectId:$subscription: $partitions")
        (partitions, numUndeliveredMessages)
      } else
        (MinPartitions, DefaultNumRecords)
    } catch {
      case e: Exception =>
        logError(s"Error calculating partitions for $projectId:$subscription, " +
          s"defaulting to $MinPartitions partitions", e)
        (MinPartitions, DefaultNumRecords)
    }
  }

  private def calculatePartitionsByRegion(projectId: String,
                                          subscription: String): List[PartitioningInfoForRegion] = {
    val request = ListTimeSeriesRequest
      .newBuilder()
      .setName(s"projects/${MetricHostProject.getOrElse(projectId)}")
      .setFilter(
        s""" metric.type="pubsub.googleapis.com/subscription/num_unacked_messages_by_region"
           |AND resource.labels.subscription_id="$subscription"
           |AND resource.label.project_id="$projectId" """.stripMargin)
      .setInterval(TimeInterval.newBuilder()
        .setEndTime(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000))
        .setStartTime(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000 - 600))
        .build())
      .setAggregation(Aggregation.newBuilder().clear().build())
      .setView(ListTimeSeriesRequest.TimeSeriesView.FULL)
      .build()

    try {
      val res = metricServiceClient.listTimeSeries(request)
      val data = res
        .getPage
        .getValues
        .iterator()
        .asScala
        .map {
          point =>
            val region = point.getMetric.getLabelsMap.get("region")
            val numUnAckedMessages = point.getPoints(0).getValue.getInt64Value
            val partitions = Math.max(
              Math.min(
                Math.ceil(numUnAckedMessages / BacklogThreshold).toInt,
                MaxPartitions),
              MinPartitions)
            PartitioningInfoForRegion(Region(region), partitions, numUnAckedMessages)
        }.toList


      if (data.nonEmpty) {
        logInfo(s"Backlog stats for $projectId:$subscription :: " +
          s"Total unacked messages: ${data.map(_.numUndeliveredMessages).sum}, " +
          s"Region-wise: ${
            data
              .map(d => s"${d.region} :: " +
                s"Unacked messages: ${d.numUndeliveredMessages}, " +
                s"Partitions: ${d.numPartitions}")
              .mkString("[", " | ", "]")
          }")
        data
      } else
        List.empty
    } catch {
      case e: Exception =>
        logError(s"Error calculating partitions for $projectId:$subscription", e)
        List.empty
    }
  }

  private val timer = new Timer("PubsubSubscriptionMonitor", true)

  timer.schedule(
    new java.util.TimerTask {
      def run(): Unit = updatePartitions()
    },
    PubsubMonitoringInterval,
    PubsubMonitoringInterval
  )

  // Register for shutdown on termination
  ShutdownHookManager.addShutdownHook(() => timer.cancel())
}

package org.apache.spark.sql.connector.pubsub

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PubsubOptionsTest extends AnyFunSuite with Matchers {

  test("validateAndInitReadOptions should throw an exception when the project id is not set") {
    val options = Map(
      "subscription" -> "test-subscription"
    )
     intercept[IllegalArgumentException] {
      validateAndInitReadOptions(options)
    }
  }

  test("validateAndInitReadOptions should throw an exception when the subscription is not set") {
    val options = Map(
      "project_id" -> "test-project"
    )
    intercept[IllegalArgumentException] {
      validateAndInitReadOptions(options)
    }
  }

  test("validateAndInitReadOptions should return PubsubReadOptions when the project id and subscription are set") {
    val options = Map(
      "project_id" -> "test-project",
      "subscription" -> "test-subscription"
    )
    val pubsubReadOptions = validateAndInitReadOptions(options)
    pubsubReadOptions.pubsubProjectId should be("test-project")
    pubsubReadOptions.pubsubSubscription should be("test-subscription")
    pubsubReadOptions.numPartitions should be(4)
    pubsubReadOptions.maxMessagesPerPartition should be(1000)
  }

  test("validateAndInitReadOptions should return PubsubReadOptions when all options are set") {
    val options = Map(
      "project_id" -> "test-project",
      "subscription" -> "test-subscription",
      "num_partitions" -> "10",
      "max_messages_per_partition" -> "5000"
    )
    val pubsubReadOptions = validateAndInitReadOptions(options)
    pubsubReadOptions.pubsubProjectId should be("test-project")
    pubsubReadOptions.pubsubSubscription should be("test-subscription")
    pubsubReadOptions.numPartitions should be(10)
    pubsubReadOptions.maxMessagesPerPartition should be(5000)
  }

  test("validateAndInitReadOptions should throw an exception when max_messages_per_partition is less than 1") {
    val options = Map(
      "project_id" -> "test-project",
      "subscription" -> "test-subscription",
      "max_messages_per_partition" -> "0"
    )
    intercept[IllegalArgumentException] {
      validateAndInitReadOptions(options)
    }
  }

  test("validateAndInitReadOptions should throw an exception when num_partitions is less than 1") {
    val options = Map(
      "project_id" -> "test-project",
      "subscription" -> "test-subscription",
      "num_partitions" -> "0"
    )
    intercept[IllegalArgumentException] {
      validateAndInitReadOptions(options)
    }
  }

  test("validateAndInitWriteOption should throw an exception when the project id is not set") {
    val options = Map(
      "topic" -> "test-topic"
    )
    intercept[IllegalArgumentException] {
      validateAndInitWriteOption(options)
    }
  }

  test("validateAndInitWriteOption should throw an exception when the topic is not set") {
    val options = Map(
      "project_id" -> "test-project"
    )
    intercept[IllegalArgumentException] {
      validateAndInitWriteOption(options)
    }
  }

  test("validateAndInitWriteOption should return PubsubWriteOptions when the project id and topic are set") {
    val options = Map(
      "project_id" -> "test-project",
      "topic" -> "test-topic"
    )
    val pubsubWriteOptions = validateAndInitWriteOption(options)
    pubsubWriteOptions.pubsubProjectId should be("test-project")
    pubsubWriteOptions.pubsubTopic should be("test-topic")
    pubsubWriteOptions.orderingKey should be(None)
  }

  test("validateAndInitWriteOption should return PubsubWriteOptions when all options are set") {
    val options = Map(
      "project_id" -> "test-project",
      "topic" -> "test-topic",
      "ordering_key" -> "test-key"
    )
    val pubsubWriteOptions = validateAndInitWriteOption(options)
    pubsubWriteOptions.pubsubProjectId should be("test-project")
    pubsubWriteOptions.pubsubTopic should be("test-topic")
    pubsubWriteOptions.orderingKey should be(Some("test-key"))
  }
}

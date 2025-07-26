package org.apache.spark.sql.connector.pubsub

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.util.CollectionAccumulator

case class PubsubInputPartition(rddBlockId: RDDBlockId,
                                options: PubsubReadOptions,
                                ackIdAccumulator: CollectionAccumulator[String],
                                endpointOverride: Option[String] = None) extends InputPartition

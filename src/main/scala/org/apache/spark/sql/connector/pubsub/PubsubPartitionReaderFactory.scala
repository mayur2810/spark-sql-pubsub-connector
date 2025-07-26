package org.apache.spark.sql.connector.pubsub

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}


class PubsubPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PubsubPartitionReader = {
    new PubsubPartitionReader(partition.asInstanceOf[PubsubInputPartition])
  }
}
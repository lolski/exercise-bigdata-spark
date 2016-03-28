package lolski

import org.apache.spark.Partitioner

/**
  * Created by lolski on 3/28/16.
  */

class PartitionByCountryCode(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) must be >= 0")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = key match {
    case key: LogEntry => key.countryCode.hashCode % numPartitions
  }
}
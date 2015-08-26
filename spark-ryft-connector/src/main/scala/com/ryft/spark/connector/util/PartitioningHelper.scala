package com.ryft.spark.connector.util

import com.ryft.spark.connector.config.ConfigHolder
import com.ryft.spark.connector.domain.RyftPartition

object PartitioningHelper {
  /**
   * Chooses partitions, based on specified query.
   * If partition metadata does not exist it will be added to result list.
   * Assumed that partitions with no metadata should be used by default.
   *
   * @param query Query string
   * @return Seq of partitions
   */
  def choosePartitions(query: String): List[RyftPartition] = {
    val partitions = ConfigHolder.getConf
    partitions.filter({ryft =>
      // we have to use all partitions with no pattern (metadata) (by default)
      // or pattern matches search query
      ryft.pattern.isEmpty ||
        //FIXME: regex not matches whole query
        query.head.toString.matches(ryft.pattern.replaceAll("\"", ""))
    })
  }
}
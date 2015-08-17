package com.ryft.spark.connector.util

import com.typesafe.config.{ConfigObject, ConfigValue, ConfigFactory, Config}
import scala.collection.JavaConverters._
import java.util.Map.Entry


case class RyftPartition(url: String, pattern: String)

class PartitioningHelper {
  private lazy val config = ConfigFactory.load().getConfig("spark-ryft-connector")
  private lazy val partitions: Map[String, String] = {
    val list: Iterable[ConfigObject] = config.getObjectList("partitions").asScala
    (for {
      item: ConfigObject <- list
      entry: Entry[String, ConfigValue] <- item.entrySet().asScala
      url = entry.getKey
      pattern = entry.getValue.unwrapped().toString
    } yield (url, pattern)).toMap
  }

  /**
   * Chooses partitions, based on specified query.
   * If partition metadata does not exist it will be added to result list.
   * Assumed that partitions with no metadata should be used by default.
   *
   * @param query Query string
   * @return List of partitions
   */
  def choosePartitions(query: String): List[RyftPartition] = {
    partitions.filter({case (url, pattern) =>
      // we have to use all partitions with no pattern (metadata) (by default)
      // or pattern matches search query
      pattern.isEmpty || {
        val Pattern = pattern.r
        query.head match {
          case Pattern(_) => true
          case _          => false
        }
    }}).map({case (url, pattern) => RyftPartition(url, pattern)}).toList
  }
}

object PartitioningHelper extends PartitioningHelper
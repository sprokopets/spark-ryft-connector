package com.ryft.spark.connector.config

import com.ryft.spark.connector.domain.RyftPartition
import com.typesafe.config._

import scala.collection.JavaConverters._

class ConfigHolder {
  val list = ConfigFactory.load()
    .getConfig("spark-ryft-connector")
    .getObjectList("partitions").asScala

  val partitionsConfigList = (for {
      item: ConfigObject <- list
      endpoint = item.get("endpoint").render.replaceAll("\"","")
      pattern  = item.get("pattern").render.replaceAll("\"","")
      preferredLocations = item.toConfig.getStringList("preferredLocations")
    } yield RyftPartition(endpoint, pattern, preferredLocations.asScala)).toList
}

object ConfigHolder extends ConfigHolder {
  def getConf = partitionsConfigList
}
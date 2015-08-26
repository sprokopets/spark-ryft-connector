package com.ryft.spark.connector.domain

case class RyftPartition(endpoint: String,
                    pattern: String,
                    preferredLocations: Seq[String])

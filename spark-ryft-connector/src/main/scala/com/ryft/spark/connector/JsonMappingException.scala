package com.ryft.spark.connector

import org.apache.spark.Logging

class JsonMappingException(e: Exception, line: String) extends Exception with Logging{
  log.warn(s"Unable to map line: '$line'", e)
}

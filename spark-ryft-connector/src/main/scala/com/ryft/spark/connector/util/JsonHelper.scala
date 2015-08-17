package com.ryft.spark.connector.util

import org.json4s.JsonAST.JObject
import org.json4s.native.JsonMethods._
import org.json4s.DefaultFormats

object JsonHelper {
  implicit val formats = DefaultFormats

  def toJsonPretty(obj: JObject) = pretty(render(obj))
}

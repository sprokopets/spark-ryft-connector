package com.ryft.spark.connector.util

object UrlEncoder {
  def encode(s: String) = java.net.URLEncoder.encode(s, "utf-8").replace("+","%20")
}

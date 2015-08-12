package com.ryft.spark.connector.examples

case class SampleObject (file: String, offset: Int, length: Int, fuzziness: Byte, data: String) {
  def this() = this("", 0, 0, 0, "")

  override def toString: String = data
  def nonEmpty = data.nonEmpty
}

package com.ryft.spark.connector.examples

import java.io.{BufferedWriter, FileWriter}

import com.ryft.spark.connector.RyftConnectorApi
import org.apache.commons.codec.binary.Base64
import org.apache.spark.{SparkContext, SparkConf}

import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._

import scala.util.Try


case class SampleObject (file: String, offset: Int, length: Int, fuzziness: Byte, data: String) {
  def this() = this("", 0, 0, 0, "")

  override def toString: String = "data: " + data
  def nonEmpty = data.nonEmpty
}


/**
 * Demonstrates exact search using fuzzy = 0
 */
object SimpleExample extends App{
  implicit val formats = DefaultFormats

  def toSampleObject(s: String): SampleObject = {
    val res = Try(parse(s).extract[SampleObject])
    if (res.isSuccess) {
      val sampleObj: SampleObject = res.get
      new SampleObject(sampleObj.file, sampleObj.offset, sampleObj.length, sampleObj.fuzziness,
        new String(Base64.decodeBase64(sampleObj.data)))
    } else new SampleObject
  }

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("TwitterSportTag")

  val sc = new SparkContext(sparkConf)

  val fw = new FileWriter("/result.log", true)
  val out = new BufferedWriter(fw)

  val ryftConnApi = new RyftConnectorApi(sc)

  val result = ryftConnApi.fuzzy[SampleObject](List("Jones"), List("passengers.txt"), 10, 0, toSampleObject)
  val first = result.seq(0)
  first._2.foreach(f => {
    if (f.nonEmpty) {
      out.write(f.toString)
      out.newLine()
    }
  })

  out.flush()
}

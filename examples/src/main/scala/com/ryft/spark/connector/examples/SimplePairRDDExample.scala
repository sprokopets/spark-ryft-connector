package com.ryft.spark.connector.examples

import java.io.{BufferedWriter, FileWriter}

import org.apache.commons.codec.binary.Base64
import org.apache.spark.{SparkContext, SparkConf}
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._

import scala.util.Try

object SimplePairRDDExample extends App {
  implicit val formats = DefaultFormats

  def toSampleObject(s: String): SampleObject = {
    val open = s.indexOf('{')
    if (open != -1) {
      val close = s.lastIndexOf('}')
      val ss = s.substring(open, close+1)
      val res = Try(parse(ss).extract[SampleObject])
      if (res.isSuccess) {
        val sampleObj: SampleObject = res.get
        new SampleObject(sampleObj.file, sampleObj.offset, sampleObj.length, sampleObj.fuzziness,
          new String(Base64.decodeBase64(sampleObj.data)))
      } else new SampleObject
    } else new SampleObject
  }

  val sparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("TwitterSportTag")

  val sc = new SparkContext(sparkConf)

  val fw = new FileWriter("/result1.log", true)
  val out = new BufferedWriter(fw)

  val ryftRDD = sc.ryftPairRDD[SampleObject](List("Jones","Thomas"),
    List("passengers.txt"), 10, 0, toSampleObject)

  ryftRDD.foreach(rdd => {
    if (rdd._2.nonEmpty) {
      //FIXME: synchronized file writing only for example, need to do it in a better way
      synchronized {
        out.write(rdd._1+": ")
        out.write(rdd._2.toString)
        out.newLine()
        out.flush()
      }
    }
  })

}

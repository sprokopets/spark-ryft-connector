package com.ryft.spark.connector.examples

import java.io.{BufferedWriter, FileWriter}
import com.ryft.spark.connector.domain.RyftData
import com.ryft.spark.connector.util.RyftHelper
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.json4s.DefaultFormats

import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, Future}

object TwitterExample extends App {
  implicit val formats = DefaultFormats
  implicit val ec = ExecutionContext.fromExecutor(
    Executors.newFixedThreadPool(5))

  System.setProperty("twitter4j.oauth.consumerKey", "")
  System.setProperty("twitter4j.oauth.consumerSecret", "")
  System.setProperty("twitter4j.oauth.accessToken", "")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "")

  val sparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("TwitterSportTag")

  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(10))

  val fw = new FileWriter("/result.log", true)
  val out = new BufferedWriter(fw)

  val stream = TwitterUtils.createStream(ssc, None, List("#football"), StorageLevel.MEMORY_AND_DISK_SER_2)
  val hashTags = stream.flatMap(status => status.getText.split(" ")
    .filter(_.startsWith("#")))

  val topCounts10 = hashTags.map((_, 1))
    .reduceByKeyAndWindow(_ + _, Seconds(10))
    .map{case (topic, count) => (count, topic)}
    .transform(_.sortByKey(false))

  topCounts10.foreachRDD(rdd => {
    val popularAmount = 5
    if (rdd.count() > 0) {
      val topList = rdd.take(popularAmount)
      println("\nPopular %s topics in last 10 seconds (%s total):".format(popularAmount, rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
      val tags = topList.map(e => e._2.substring(1, e._2.length)).toList

      out.write("Looking for tags: "+tags.mkString(","))
      out.newLine()

      val ryftRDD = sc.ryftPairRDD[RyftData](tags, List("reddit/*"), 10, 0, RyftHelper.mapToRyftData)
      val count = ryftRDD.countByKey()

      out.write(count.mkString("\n"))
      out.newLine()
      out.write("---------")
      out.newLine()
      out.flush()
  }})

  ssc.start()
  ssc.awaitTermination()
}

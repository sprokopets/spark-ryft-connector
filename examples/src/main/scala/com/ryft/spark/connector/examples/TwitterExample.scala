package com.ryft.spark.connector.examples

import java.io.{BufferedWriter, FileWriter}
import java.util.concurrent.{Callable, FutureTask, Executors}
import com.ryft.spark.connector.domain.RyftData
import com.ryft.spark.connector.util.RyftHelper
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.json4s.DefaultFormats

import scala.concurrent.ExecutionContext

object TwitterExample extends App {
  implicit val formats = DefaultFormats
  implicit val ec = ExecutionContext.fromExecutor(
    Executors.newFixedThreadPool(10))

  System.setProperty("twitter4j.oauth.consumerKey", "")
  System.setProperty("twitter4j.oauth.consumerSecret", "")
  System.setProperty("twitter4j.oauth.accessToken", "")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "")

  val sparkConf = new SparkConf()
    .setAppName("TwitterSportTag")
    .set("spark.locality.wait", "120s")
    .set("spark.locality.wait.node", "120s")

  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(30))

  val stream = TwitterUtils.createStream(ssc, None, List("#football"), StorageLevel.MEMORY_AND_DISK_SER_2)
  val hashTags = stream.flatMap(status => status.getText.split(" ")
    .filter(_.startsWith("#")))

  val topCounts10 = hashTags.map((_, 1))
    .reduceByKeyAndWindow(_ + _, Seconds(30))
    .map{case (topic, count) => (count, topic)}
    .transform(_.sortByKey(false))

  topCounts10.foreachRDD(rdd => {
    val popularAmount = 4
    if (rdd.count() > 0) {
      val topList = rdd.take(popularAmount)
      println("\nPopular %s topics in last 30 seconds (%s total):".format(popularAmount, rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
      val tags = topList.map(e => e._2.substring(1, e._2.length)).toList

      val ryftRDD = sc.ryftPairRDD[RyftData](tags, List("reddit/*"), 10, 0, RyftHelper.mapToRyftData)
      val count = ryftRDD.countByKey()
      println("\n"+count.mkString("\n"))
      println("---------\n")
    }
  })

  ssc.start()
  ssc.awaitTermination()
}

/*
 * ============= Ryft-Customized BSD License ============
 * Copyright (c) 2015, Ryft Systems, Inc.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
 *   This product includes software developed by Ryft Systems, Inc.
 * 4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
 *   to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * ============
 */

package com.ryft.spark.connector.examples

import com.ryft.spark.connector._
import com.ryft.spark.connector.domain.RyftQueryOptions
import com.ryft.spark.connector.domain.query.SimpleRyftQuery

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.json4s.DefaultFormats

object TwitterExample extends App with Logging {
  implicit val formats = DefaultFormats
  val timeWindow = 30
  val filter = "football"
  val popularAmount = 4
  val metaInfo = RyftQueryOptions(List("reddit/*"), 10, 0)

  System.setProperty("twitter4j.oauth.consumerKey", "")
  System.setProperty("twitter4j.oauth.consumerSecret", "")
  System.setProperty("twitter4j.oauth.accessToken", "")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "")

  val sparkConf = new SparkConf()
    .setAppName("TwitterSportTag")
    .set("spark.locality.wait", "120s")
    .set("spark.locality.wait.node", "120s")

  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(timeWindow))

  val stream = TwitterUtils.createStream(ssc, None, List("#"+filter), StorageLevel.MEMORY_AND_DISK_SER_2)
  val hashTags = stream.flatMap(status => status.getText.split(" ")
    .filter(_.startsWith("#")))

  val topCountsN = hashTags.map((_, 1))
    .reduceByKeyAndWindow(_ + _, Seconds(timeWindow))
    .map{case (topic, count) => (count, topic)}
    .transform(_.sortByKey(false))

  topCountsN.foreachRDD(rdd => {
    if (rdd.count() > 0) {
      val topList = rdd.take(popularAmount)
      logInfo("\nPopular %s topics in last %s seconds (%s total):"
        .format(popularAmount, timeWindow, rdd.count()))
      topList.foreach { case (count, tag) => logInfo("%s (%s tweets)".format(tag, count)) }
      val tags = topList.map(e => e._2.substring(1, e._2.length)).toList
      val queries = tags.map(t => SimpleRyftQuery(List(t)))
      val ryftRDD = sc.ryftPairRDD(queries, metaInfo)

      val count = ryftRDD.countByKey()
      logInfo("\n"+count.mkString("\n"))
    }
  })

  ssc.start()
  ssc.awaitTermination()
}

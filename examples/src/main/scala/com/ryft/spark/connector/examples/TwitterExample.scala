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
import com.ryft.spark.connector.domain.{RyftData, RyftQueryOptions}
import com.ryft.spark.connector.query.SimpleQuery
import com.ryft.spark.connector.rdd.RyftPairRDD
import org.apache.spark.rdd.PairRDDFunctions

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.json4s.DefaultFormats

import scala.language.postfixOps
import com.ryft.spark.connector._

object TwitterExample extends App with Logging {
  implicit val formats = DefaultFormats
  val timeWindow = 60
  val filter = "#Election2016"
  val popularAmount = 4
  val queryOptions = RyftQueryOptions("reddit/*", 10, 0 toByte)

  System.setProperty("twitter4j.oauth.consumerKey", "YZvJEtKXN7QuwpjFNw")
  System.setProperty("twitter4j.oauth.consumerSecret", "MkgdujvCbXyiF5JNW66XV5S0SOvpxd6UPekp9c3RN7Y")
  System.setProperty("twitter4j.oauth.accessToken", "1449688434-T4qw1c5TvAwc2rtNExI3caBUTX8L93ab2cuGZvh")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "TOLfaalYuQfNCNKVWOjJpoJcDEesdTPgEi9WWKrohCk")

  val candidates = Seq(
    "Trump", "Carson", "Rubio", "Cruz", "Fiorina", "Huckabee",
    "Paul", "Christie", "Kasich", "Santorum", "Graham", "Jindal",
    "Pataki", "Bush", "Clinton", "Sanders", "Biden", "Chafee",
    "Webbv", "O’Malley", "Lessig")

  val sparkConf = new SparkConf()
    .setAppName("TwitterSportTag")
    .setMaster("local[2]")
    .set("spark.locality.wait", "120s")
    .set("spark.locality.wait.node", "120s")

  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(timeWindow))

  val stream = TwitterUtils.createStream(ssc, None, List(filter), StorageLevel.MEMORY_AND_DISK_SER_2)
  val mentionedCandidates = stream.flatMap(status => status.getText.split(" "))
    .filter(candidates.contains)

  mentionedCandidates.foreachRDD(rdd => {
    val rddCount = rdd.map(word => (word, 1))
      .reduceByKey((a, b) => a + b)

    val candidates = rdd.collect().toList
    val simpleQueries = candidates.map(SimpleQuery(_))
    val ryftRDD = sc.ryftPairRDD(simpleQueries, queryOptions)
  })

  ssc.start()
  ssc.awaitTermination()
}

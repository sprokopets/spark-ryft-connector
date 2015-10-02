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

import com.ryft.spark.connector.domain.{contains, recordField, RyftQueryOptions}
import com.ryft.spark.connector.query.{RyftQuery, RecordQuery}
import com.ryft.spark.connector.rdd.RyftRDDSimple
import com.ryft.spark.connector.util.PartitioningHelper
import org.apache.spark.{SparkContext, SparkConf, Logging}

import com.ryft.spark.connector._

object StructuredRDDExample extends App with Logging {
  val sparkConf = new SparkConf()
    .setAppName("SimplePairRDDExample")

  val sc = new SparkContext(sparkConf)

  val query =
    RecordQuery(
      RecordQuery(recordField("desc"), contains, "VEHICLE")
      .or(recordField("desc"), contains, "KE")
      .or(recordField("desc"), contains, "TO"))
    .and(RecordQuery(recordField("date"), contains, "04/15/2015"))

  val ryftOptions = RyftQueryOptions(List("*.pcrime"), 0, 0)
  val ryftRDD = sc.ryftRDD(List(query),ryftOptions, PartitioningHelper.byFirstLetter,
  preferredNode)

  val countByDescription = ryftRDD.asInstanceOf[RyftRDDSimple[Map[String, String]]]
    .map(m => {
      (m.get("LocationDescription"), 1)
    }).reduceByKey(_ + _)

  countByDescription.foreach({case(key, count) =>
    println("key: "+key.get+" count: "+count)
  })

  def preferredNode(preferredPartition: String): Set[String] = {
    preferredPartition match {
      case "http://52.20.99.136:9000" => Set("172.16.92.4","172.16.92.5")
      case "http://52.20.99.136:8765" => Set("172.16.92.6","172.16.92.7")
      case _ =>
        logDebug("Unable to find preferred spark node")
        Set.empty[String]
    }
  }
}

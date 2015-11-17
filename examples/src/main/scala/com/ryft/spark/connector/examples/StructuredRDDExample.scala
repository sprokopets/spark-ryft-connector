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
import com.ryft.spark.connector.query.RecordQuery
import com.ryft.spark.connector.rdd.RyftRDD
import com.ryft.spark.connector.util.RyftPartitioner
import org.apache.spark.{SparkContext, SparkConf, Logging}

import com.ryft.spark.connector._

object StructuredRDDExample extends App with Logging {
  val sparkConf = new SparkConf()
    .setAppName("SimplePairRDDExample")
    .setMaster("local[2]")

  val sc = new SparkContext(sparkConf)

  val query = RecordQuery(recordField("Description"), contains, "VEHICLE")

  val ryftOptions = RyftQueryOptions("*.pcrime", structured = true)
  val ryftRDD = sc.ryftRDD(Seq(query),ryftOptions)

  val countByDescription = ryftRDD.asInstanceOf[RyftRDD[Map[String, String]]]
    .map(m => {
    (m.get("LocationDescription"), 1)
  }).reduceByKey(_ + _)

  countByDescription.foreach({case(key, count) =>
    println("key: "+key.get+" count: "+count)
  })
}

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

package com.ryft.spark.connector.rdd

import _root_.spray.json.DefaultJsonProtocol
import _root_.spray.json.JsArray
import _root_.spray.json.JsNumber
import _root_.spray.json.JsObject
import _root_.spray.json.JsString
import com.ryft.spark.connector.domain.RyftQueryOptions
import com.ryft.spark.connector.rest.RyftRestConnection
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD
import spray.json._
import DefaultJsonProtocol._

import scala.reflect.ClassTag
import scala.util.Try

/**
 * RDD representing of a RyftQuery.
 *
 * This class is the main entry point for analyzing data using Ryft with Spark.
 */
abstract class RyftAbstractRDD [T: ClassTag, R](@transient sc: SparkContext,
    val rddQueries: Seq[RDDQuery],
    val queryOptions: RyftQueryOptions)
  extends RDD[T](sc, Nil) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T]

  override def count(): Long = {
    Try(ryftCount()).getOrElse{
      logInfo("Ryft Rest count endpoint failed. Using default spark count function")
      super.count()
    }
  }

  private def ryftCount(): Long = {
    getPartitions.map { p =>
      //FIXME: need not to do replacing in existing query string
      //insted of this, have to create more configurable query
      val query = p.asInstanceOf[RyftRDDPartition].query
        .replace("/search?", "/count?")
      new RyftRestConnection(query).result.trim.toLong
    }.sum
  }

  override protected def getPartitions: Array[Partition] = {
    val partitioner = new RyftRDDPartitioner
    val partitions = partitioner.partitions(rddQueries)
    logDebug(s"Created total ${partitions.length} partitions.")
    logDebug("Partitions: \n" + partitions.mkString("\n"))
    partitions
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val partition = split.asInstanceOf[RyftRDDPartition]
    logDebug(("Preferred locations for partition:" +
      "\npartitions idx: %s" +
      "\nlocations: %s")
      .format(partition.idx, partition.preferredLocations.mkString("\n")))
    partition.preferredLocations.toSeq
  }
}

/**
 * Describes `RyftRDD` partition
 *
 * @param idx Identifier of the partition, used internally by Spark
 * @param key Search query key
 * @param query Ryft REST query
 * @param preferredLocations Preferred spark workers
 */
case class RyftRDDPartition(idx: Int,
    key: String,
    query: String,
    preferredLocations: Set[String])
  extends Partition {
  override def index: Int = idx
  override def toString: String = JsObject(
    Map(
      "idx" -> JsNumber(idx),
      "key" -> JsString(key),
      "query" -> JsString(query),
      "preferredLocations" -> JsArray(preferredLocations.map(_.toJson).toVector)
    )).toJson.prettyPrint
}

/**
 * Simple `RyftRDD` partitioner to prepare partitions
 */
class RyftRDDPartitioner {
  def partitions(rddQueries: Seq[RDDQuery]): Array[Partition] = {
    var index = 0
    (for (rddQuery <- rddQueries) yield {
      (for (query <- rddQuery.ryftRestQueries) yield {
        val partition = new RyftRDDPartition(index, rddQuery.key, query, rddQuery.preferredLocations)
        index += 1
        partition
      }).toArray[Partition]
    }).flatten.toArray[Partition]
  }
}
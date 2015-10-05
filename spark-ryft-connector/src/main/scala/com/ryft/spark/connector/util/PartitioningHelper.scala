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

package com.ryft.spark.connector.util

import com.ryft.spark.connector.RyftSparkException
import com.ryft.spark.connector.config.ConfigHolder
import com.ryft.spark.connector.query._
import org.apache.spark.Logging
import scala.reflect.runtime.universe._

import scala.annotation.tailrec
import scala.collection.mutable

/**
 * Default Partitioning mechanism.
 */
object PartitioningHelper extends Logging {

  /**
   * Chooses partitions according to first letter of the query
   * @param recordQuery Search query
   * @return Set of partitions for query
   */
  def byFirstLetter(recordQuery: RyftQuery) = {
    recordQuery match {
      case sq: SimpleQuery => sq.queries.flatMap(q => partitionsByFirstLetter(q)).toSet
      case rq: RecordQuery => choosePartitions(rq.queries, mutable.Set.empty[String],
        partitionsByFirstLetter)
      case _ =>
        val msg = "Unable to find partitions for RyftQuery. " +
          "Unrecognized RyftQuery subtype: " + typeOf[RyftQuery]
        logWarning(msg)
        throw new RyftSparkException(msg)
    }
  }

  /**
   * Chooses partitions using function passed for partitioning
   *
   * @param ryftQuery Ryft query
   * @param partitions Function to choose partition
   * @return Set of partitions required for given query
   */
  def choosePartitions(ryftQuery: RyftQuery, partitions: String => Set[String]): Set[String] = {
    ryftQuery match {
      case sq: SimpleQuery => sq.queries.flatMap(q => partitions(q)).toSet
      case rq: RecordQuery => choosePartitions(rq.queries, mutable.Set.empty[String], partitions)
      case _ =>
        val msg = "Unable to find partitions for RyftQuery. " +
          "Unrecognized RyftQuery subtype: " + typeOf[RyftQuery]
        logWarning(msg)
        throw new RyftSparkException(msg)
    }
  }

  @tailrec
  private def choosePartitions(
    queries: List[GenericQuery],
    acc: mutable.Set[String],
    partitions: String => Set[String]):
  Set[String] = {

    if (queries.isEmpty) acc.toSet
    else {
      queries.head match {
        case sq: SingleQuery =>
          acc ++= partitions(sq.query)
          choosePartitions(queries.tail, acc, partitions)

        case rq: RecordQuery =>
          acc ++= recordQueryPartitions(rq, partitions)
          choosePartitions(queries.tail, acc, partitions)

        case _ =>
          val msg = "Unable to choose partitions. " +
            "Unexpected Ryft Query Type: " + typeOf[GenericQuery]
          logWarning(msg)
          throw new RyftSparkException("Unexpected Ryft Query Type")
      }
    }
  }

  private def recordQueryPartitions(
    rq: RecordQuery,
    partitions: String => Set[String]):
  Set[String] = {
    choosePartitions(rq.queries, mutable.Set.empty[String], partitions)
  }

  private def partitionsByFirstLetter(query: String): Set[String] = {
    ConfigHolder.partitions.filter({
      case(url, pattern) => pattern.isEmpty || {
        val Pattern = pattern.r.unanchored
        query match {
          case Pattern(_) => true
          case _          => false
        }
      }
    }).keySet
  }
}
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

import com.ryft.spark.connector.config.ConfigHolder
import com.ryft.spark.connector.exception.RyftSparkException
import com.ryft.spark.connector.query._
import com.ryft.spark.connector.query.filter._
import org.apache.spark.Logging

import scala.annotation.tailrec
import scala.reflect.runtime.universe._

/**
 * Default Partitioning mechanism.
 */
object RyftPartitioner extends Logging {

  /**
   * Chooses partitions using function passed for partitioning
   *
   * @param ryftQuery Ryft query
   * @param partFunc Function to choose partition
   * @return Set of partitions required for given query
   */
  def forRyftQuery(ryftQuery: RyftQuery,
      partFunc: String => List[String] = _ => List.empty[String],
      recordFields: Seq[String] = Seq.empty[String]):
  Set[String] = {
    ryftQuery match {
      case sq: SimpleQuery => sq.queries.flatMap(q => partFunc(q)).toSet
      case rq: RecordQuery => forFilters(rq.filters, partFunc, Nil, recordFields.map("RECORD." + _))
      case _ =>
        val msg = "Unable to find partitions for RyftQuery. " +
          "Unrecognized RyftQuery subtype: " + typeOf[RyftQuery]
        logWarning(msg)
        throw new RyftSparkException(msg)
    }
  }

  /**
   * Chooses partitions according to first letter of the query
   * @param query Search query
   * @return Set of partitions for query
   */
  def byFirstLetter(query: String): List[String] = {
    ConfigHolder.partitions.filter {
      case(url, pattern) => pattern.isEmpty || {
        val Pattern = pattern.r.unanchored
        query match {
          case Pattern(_) => true
          case _          => false
        }
      }
    }.keys.toList
  }

  @tailrec
  private def forFilters(filters: List[Filter],
      partFunc: String => List[String],
      acc: List[String] = Nil,
      recordFields: Seq[String]): Set[String] = {
    if (filters.isEmpty) acc.toSet
    else {
      val preferredPartitions = partitions(filters.head :: Nil, partFunc, Nil, recordFields)
      forFilters(filters.tail, partFunc, preferredPartitions ::: acc, recordFields)
    }
  }

  @tailrec
  private def partitions(f: List[Filter],
      partFunc: String => List[String],
      acc: List[String],
      recordFields: Seq[String] ): List[String] = f match {
    case EqualTo(attr, value) :: tail =>
      val ps = partitionsByRecordFields(attr, value, partFunc, recordFields)
      partitions(tail, partFunc,  ps ::: acc, recordFields)

    case Contains(attr, value) :: tail => val ps =
      partitionsByRecordFields(attr, value, partFunc, recordFields)
      partitions(tail, partFunc,  ps ::: acc, recordFields)

    case NotEqualTo(attr, value) :: tail =>
      val ps = partitionsByRecordFields(attr, value, partFunc, recordFields)
      partitions(tail, partFunc,  ps ::: acc, recordFields)

    case NotContains(attr, value) :: tail =>
      val ps = partitionsByRecordFields(attr, value, partFunc, recordFields)
      partitions(tail, partFunc,  ps ::: acc, recordFields)

    case And(left, right) :: tail => partitions(left :: right :: tail, partFunc, acc, recordFields)
    case Or(left, right) :: tail => partitions(left :: right :: tail, partFunc, acc, recordFields)
    case Xor(left, right) :: tail => partitions(left :: right :: tail, partFunc, acc, recordFields)
    case _ => acc
  }

  private def partitionsByRecordFields(attr: String,
      value: String,
      partFunc: String => List[String],
      recordFields: Seq[String]): List[String] = {

    if (recordFields.isEmpty) partFunc(value)
    else if (recordFields.contains(attr)) partFunc(value)
    else Nil
  }
}
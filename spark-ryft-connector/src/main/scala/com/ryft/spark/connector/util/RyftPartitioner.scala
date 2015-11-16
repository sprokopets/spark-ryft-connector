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

  def byField(query: RyftQuery,
      partFunc: String => List[String],
      fields: Seq[String]): List[String] = query match {
    case rq: RecordQuery => forFilters(rq.filters, partFunc, fields)
    case _ =>
      val msg = "Unable to find partitions for RyftQuery. " +
        "Unrecognized RyftQuery subtype: " + typeOf[RyftQuery]
      logWarning(msg)
      throw new RyftSparkException(msg)
  }

  def forSimpleQuery(query: RyftQuery, partFunc: String => List[String]) = query match {
    case sq: SimpleQuery => sq.queries.flatMap(byFirstLetter)
    case _ =>
      val msg = "Unable to find partitions for RyftQuery. " +
        "Unrecognized RyftQuery subtype: " + typeOf[RyftQuery]
      logWarning(msg)
      throw new RyftSparkException(msg)
  }

  /**
   * Chooses partitions according to first letter of the query
   * @param query Search query
   * @return Set of partitions for query
   */
  def byFirstLetter(query: String): List[String] = {
    ConfigHolder.partitions.filter {
      case (url, pattern) => pattern.isEmpty || {
        val Pattern = pattern.r.unanchored
        query match {
          case Pattern(_) => true
          case _ => false
        }
      }
    }.keys.toList
  }

  @tailrec
  private def forFilters(filters: List[Filter],
      partFunc: String => List[String],
      fields: Seq[String],
      acc: List[String] = Nil): List[String] = {
    if (filters.isEmpty) acc
    else {
      val preferredPartitions = partitions(filters.head :: Nil, partFunc, fields, Nil)
      forFilters(filters.tail, partFunc, fields, preferredPartitions ::: acc)
    }
  }

  @tailrec
  private def partitions(f: List[Filter],
                         partFunc: String => List[String],
                         fields: Seq[String],
                         acc: List[String]): List[String] = f match {
    case EqualTo(attr, value) :: tail =>
      val preferredPartitions = applyPartitioning(attr, value, partFunc, fields)
      partitions(tail, partFunc, fields, preferredPartitions ::: acc)
    case Contains(attr, value) :: tail =>
      val preferredPartitions = applyPartitioning(attr, value, partFunc, fields)
      partitions(tail, partFunc, fields, preferredPartitions ::: acc)
    case NotEqualTo(attr, value) :: tail =>
      val preferredPartitions = applyPartitioning(attr, value, partFunc, fields)
      partitions(tail, partFunc, fields, preferredPartitions ::: acc)
    case NotContains(attr, value) :: tail =>
      val preferredPartitions = applyPartitioning(attr, value, partFunc, fields)
      partitions(tail, partFunc, fields, preferredPartitions ::: acc)
    case And(left, right) :: tail => partitions(left :: right :: tail, partFunc, fields, acc)
    case Or(left, right) :: tail => partitions(left :: right :: tail, partFunc, fields, acc)
    case Xor(left, right) :: tail => partitions(left :: right :: tail, partFunc, fields, acc)
    case _ => acc
  }

  private def applyPartitioning(attr: String, value: String, partFunc: String => List[String],
      fields: Seq[String]): List[String] = {
    if (fields.isEmpty) partFunc(value)
    else if (!fields.contains(attr.split("RECORD.")(1))) Nil
    else partFunc(value)
  }
}
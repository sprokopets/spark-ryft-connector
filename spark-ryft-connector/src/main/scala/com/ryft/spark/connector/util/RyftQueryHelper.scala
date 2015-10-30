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

import com.ryft.spark.connector.exception.RyftSparkException
import com.ryft.spark.connector.query.filter._
import com.ryft.spark.connector.query._
import org.apache.spark.Logging
import scala.reflect.runtime.universe._
import com.ryft.spark.connector.domain._

/**
 * Provides helper functions specific for Ryft
 */
private [connector] object RyftQueryHelper extends Logging {
  private val OR = "OR"
  private val AND = "AND"

  def keyQueryPair[RyftQuery: TypeTag](ryftQuery: RyftQuery, queryOptions: RyftQueryOptions) = {
    val ryftQueryS =
      ryftQuery match {
        case sq: SimpleQuery =>
          val queryS = s"(${queryToString(sq)})"
          val queryEncoded = encode(queryS) + queryOptionsToString(queryOptions)
          (sq.queries.mkString(","), queryEncoded)

        case rq: RecordQuery =>
          val queryString = queryToString(rq)
          val files = new StringBuilder
          queryOptions.files.foreach(f => files.append(s"&files=${encode(f)}"))
          val fields =
            if (queryOptions.fields.nonEmpty) s"&fields=${queryOptions.fields.mkString(",")}"
            else ""
          val queryEncoded = encode(queryToString(rq)) + files + "&format=xml" + fields
          (queryString, queryEncoded)

        case _ =>
          val msg = s"Unable to convert RyftQuery to string. " +
            s"Unrecognized RyftQuery subtype: ${typeOf[RyftQuery]}"
          logWarning(msg)
          throw new RyftSparkException(msg)
      }

    (ryftQueryS._1, s"/search?query=${ryftQueryS._2}")
  }

  def queryToString(query: SimpleQuery): String = {
    val queries = query.queries
    val preparedQueries = new StringBuilder(s"""(${rawText.value} ${contains.value} "${queries.head}")""")
    if (queries.tail.nonEmpty) {
      queries.tail.foreach(q => preparedQueries.append(s"""OR(${rawText.value} ${contains.value} "$q")"""))
    }

    if(queries.size > 1) s"(${preparedQueries.toString()})"
    else preparedQueries.toString()
  }

  def queryToString(rq: RecordQuery): String = {
    val result = rq.filters.map {filter =>
      if (isOneLayerTree(filter)) filterToString(filter)
      else s"""(${filterToString(filter)})"""
    }.mkString(AND)

    if (rq.filters.size > 1) s"($result)"
    else result
  }

  private def filterToString(f: Filter): String = f match {
      //TODO: implement NOT
    case EqualTo(attr, v) => s"""($attr EQUALS "$v")"""
    case NotEqualTo(attr, v) => s"""($attr NOT_EQUALS "$v")"""
    case Contains(attr, v) => s"""($attr CONTAINS "$v")"""
    case NotContains(attr, v) => s"""($attr NOT_CONTAINS "$v")"""

    case Or(left, right) =>
      val leftResult =
        if (left.isInstanceOf[And]) s"(${filterToString(left)})"
        else s"${filterToString(left)}"

      val rightResult =
        if (right.isInstanceOf[And]) s"(${filterToString(right)})"
        else s"${filterToString(right)}"

      s"$leftResult$OR$rightResult"

    case And(left, right) =>
      val leftResult =
        if (left.isInstanceOf[Or]) s"(${filterToString(left)})"
        else s"${filterToString(left)}"

      val rightResult =
        if (right.isInstanceOf[Or]) s"(${filterToString(right)})"
        else s"${filterToString(right)}"

      s"$leftResult$AND$rightResult"

    case _ =>
      println("error: "+ f)
      throw new RuntimeException //TODO: exception + message
  }

  private def isOneLayerTree(f: Filter) = f match {
    case EqualTo(attr, v) => true
    case Contains(attr, v) => true
    //TODO: Not ??
    case _ => false
  }

  private def queryOptionsToString(queryOptions: RyftQueryOptions): String = {
    val files = new StringBuilder
    queryOptions.files.foreach(f => files.append(s"&files=${encode(f)}"))
    s"$files" +
      s"&surrounding=${queryOptions.surrounding}" +
      s"&fuzziness=${queryOptions.fuzziness}"
  }

  private def encode(s: String) = java.net.URLEncoder.encode(s, "utf-8").replace("+","%20")
}
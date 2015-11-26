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

package com.ryft.spark.connector.query

import com.ryft.spark.connector.domain.{contains, rawText}
import com.ryft.spark.connector.util.RyftQueryHelper
import org.apache.spark.Logging

/**
 * Represents RyftOne RAW_TEXT type of queries.
 * @param queries Sequence of filters satisfying Ryft query
 *                Filters will be applied all together:
 *                [Filter0 AND Filter1 AND ... AND FilterN]
 */
case class SimpleQuery(queries: List[String]) extends RyftQuery with Logging {
  override def key: String = queries match {
    case head :: Nil => head
    case head :: tail :: Nil => queries.mkString("-")
    case _  =>
      logDebug("Query is empty key is empty too.")
      ""
  }

  override def values: Set[String] = queries.toSet

  override def entries: Set[(String, String)] =
    throw new UnsupportedOperationException(s"Method not supported for ${SimpleQuery.getClass.getName}")

  override def toRyftQuery: String = RyftQueryHelper.queryToString(this)
}

object SimpleQuery {
  def apply(query: String) = new SimpleQuery(List(query))
}
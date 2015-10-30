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

import com.ryft.spark.connector.domain
import com.ryft.spark.connector.domain.{InputSpecifier, RelationalOperator}
import com.ryft.spark.connector.exception.RyftSparkException
import com.ryft.spark.connector.query.filter._
import com.ryft.spark.connector.query.RecordQuery._

import scala.annotation.tailrec

/**
 * Represents Ryft query to structured data
 * @param filters Sequence of filters satisfying Ryft query
 *                Filters will be applied all all together:
 *                [Filter0 AND Filter1 AND ... AND FilterN]
 */
case class RecordQuery(filters: List[Filter])
  extends RyftQuery {

  def and(is: InputSpecifier, ro: RelationalOperator, value: String) = {
    RecordQuery(toFilter(is, ro, value) :: filters)
  }

  def and(rq: RecordQuery) = {
    RecordQuery(andToTree(rq.filters.tail, rq.filters.head) :: filters)
  }

  def or(is: InputSpecifier, ro: RelationalOperator, value: String) = {
    val filter = toFilter(is, ro, value)
    RecordQuery(Or(filter, filters.head) :: filters.tail)
  }
}

object RecordQuery {
  def apply() = new RecordQuery(Nil)
  def apply(filter: Filter) = new RecordQuery(filter :: Nil)
  def apply(is: InputSpecifier, ro: RelationalOperator, value: String) = {
    new RecordQuery(toFilter(is, ro, value) :: Nil)
  }
  def apply(rq: RecordQuery) = {
    new RecordQuery(andToTree(rq.filters.tail, rq.filters.head) :: Nil)
  }

  private def toFilter(is: InputSpecifier, ro: RelationalOperator, value: String) = ro match {
    case domain.contains    => Contains(is.value, value)
    case domain.notContains => NotContains(is.value, value)
    case domain.equalTo     => EqualTo(is.value, value)
    case domain.notEqualTo  => NotEqualTo(is.value, value)
    case _ => throw new RyftSparkException(s"Unknown Relational Operator: $ro")
  }

  @tailrec
  private def andToTree(filters: List[Filter], acc: Filter): Filter = {
    if (filters.isEmpty) acc
    else andToTree(filters.tail, And(filters.head, acc))
  }
}

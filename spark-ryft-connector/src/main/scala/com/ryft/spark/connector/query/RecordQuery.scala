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
import com.ryft.spark.connector.domain._

private[connector] sealed trait GenericQuery extends RyftQuery

private[connector] case class SingleQuery(lo: LogicalOperator,
                       is: InputSpecifier,
                       ro: RelationalOperator,
                       query: String) extends GenericQuery

private[connector] case class NestedQuery(lo: LogicalOperator,
                                      queries: List[GenericQuery])
  extends GenericQuery

case class RecordQuery(lo: LogicalOperator,
                 queries: List[GenericQuery]) extends GenericQuery {
  def this(is: InputSpecifier,
           ro: RelationalOperator,
           query: String) = {
    this(empty, SingleQuery(empty, is, ro, query) :: Nil)
  }

  def this(query: RecordQuery) = {
    this(empty, query :: Nil)
  }

  def and(is: InputSpecifier,
          ro: RelationalOperator,
          query: String) = {
    RecordQuery(empty, SingleQuery(domain.and, is, ro, query) :: queries)
  }

  def and(query: RecordQuery) = {
    RecordQuery(empty, RecordQuery(domain.and, query) :: queries)
  }

  def or(is: InputSpecifier,
         ro: RelationalOperator,
         query: String) = {
    new RecordQuery(empty, SingleQuery(domain.or, is, ro, query) :: queries)
  }

  def or(query: RecordQuery) = {
    RecordQuery(empty, RecordQuery(domain.or, query) :: queries)
  }

  def xor(is: InputSpecifier,
         ro: RelationalOperator,
         query: String) = {
    new RecordQuery(empty, SingleQuery(domain.xor, is, ro, query) :: queries)
  }

  def xor(query: RecordQuery) = {
    RecordQuery(empty, RecordQuery(domain.xor, query) :: queries)
  }

  def build = queries
}

object RecordQuery {
  def apply(is: InputSpecifier,
            ro: RelationalOperator,
            query: String) = new RecordQuery(is, ro, query)

  def apply(query: RecordQuery) = new RecordQuery(new RecordQuery(empty, query.queries))
  def apply(lo: LogicalOperator, query: RecordQuery) = new RecordQuery(lo, query.queries)
}

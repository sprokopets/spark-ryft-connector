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

package com.ryft.spark.connector

import com.ryft.spark.connector.domain.query._
import com.ryft.spark.connector._

import scala.collection.mutable

class RyftQueryBuilder(query: String,
                       inputSpecifier: InputSpecifier,
                       logicalOperator: LogicalOperator,
                       relationalOperator: RelationalOperator) {

  private val recordQueries = mutable.ListBuffer.empty[RyftRecord]

  def this(query: String,
          inputSpecifier: InputSpecifier,
          relationalOperator: RelationalOperator) = {
    this(query, inputSpecifier, empty, relationalOperator)
    recordQueries += new RyftRecord(query, inputSpecifier, empty, relationalOperator)
  }

  def and(query: String,
          inputSpecifier: InputSpecifier,
          relationalOperator: RelationalOperator) = {
    recordQueries += new RyftRecord(query, inputSpecifier, domain.query.and, relationalOperator)
    this
  }

  def or(query: String,
          inputSpecifier: InputSpecifier,
          relationalOperator: RelationalOperator) = {
    recordQueries += new RyftRecord(query, inputSpecifier, domain.query.or, relationalOperator)
    this
  }

  def xor(query: String,
          inputSpecifier: InputSpecifier,
          relationalOperator: RelationalOperator) = {
    recordQueries += new RyftRecord(query, inputSpecifier, domain.query.xor, relationalOperator)
    this
  }

  def build = new RyftRecordQuery(recordQueries.toList)
}
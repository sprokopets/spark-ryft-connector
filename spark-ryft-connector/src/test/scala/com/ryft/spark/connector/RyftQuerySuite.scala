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

import com.ryft.spark.connector.domain.RyftMetaInfo
import com.ryft.spark.connector.domain.query._
import com.ryft.spark.connector.util.RyftHelper
import org.scalatest.FunSuite

class RyftQuerySuite extends FunSuite {

  test("test ryft query builder") {
    val query = "?query=((RECORD%20CONTAINS%20%22alex%22)AND(RECORD%20EQUALS%20%22john%22)" +
      "OR(RECORD%20NOT_EQUALS%20%22martin%22))&files=passengers.txt&surrounding=10&fuzziness=2"
    val ryftQuery = new RyftQueryBuilder(record, contains, "alex")
      .and(record, domain.query.equals, "john")
      .or(record, notEquals, "martin")
      .build
    val metaInfo = new RyftMetaInfo(List("passengers.txt"), 10, 2)
    assert(query.equals(RyftHelper.queryToString(ryftQuery, metaInfo)))
  }

  test("test ryft query builder record field") {
    val query = "?query=((RECORD.field1%20CONTAINS%20%22alex%22))&files=passengers.txt&surrounding=10&fuzziness=2"
    val ryftQuery = new RyftQueryBuilder(recordField("field1"), contains, "alex")
      .build
    val metaInfo = new RyftMetaInfo(List("passengers.txt"), 10, 2)
    assert(query.equals(RyftHelper.queryToString(ryftQuery, metaInfo)))
  }

  test("test simple query") {
    val query = "?query=((RAW_TEXT%20CONTAINS%20%22Michael%22))&files=passengers.txt&surrounding=10&fuzziness=2"
    val metaInfo = new RyftMetaInfo(List("passengers.txt"), 10, 2)
    val ryftQuery = new SimpleRyftQuery(List("Michael"))
    assert(query.equals(RyftHelper.queryToString(ryftQuery, metaInfo)))
  }

  test("test few simple queries") {
    val query = "?query=((RAW_TEXT%20CONTAINS%20%22Michael%22)" +
      "OR(RAW_TEXT%20CONTAINS%20%22Alex%22))&files=passengers.txt&surrounding=10&fuzziness=2"
    val metaInfo = new RyftMetaInfo(List("passengers.txt"), 10, 2)
    val ryftQuery = new SimpleRyftQuery(List("Michael","Alex"))
    assert(query.equals(RyftHelper.queryToString(ryftQuery, metaInfo)))
  }
}

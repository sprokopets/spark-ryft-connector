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

import com.ryft.spark.connector.domain.{record, RyftQueryOptions, contains, recordField}
import com.ryft.spark.connector.query.{SimpleQuery, RecordQuery}
import com.ryft.spark.connector.util.RyftQueryHelper
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Assert._

class RyftQuerySuite extends JUnitSuite {

  val queryOptions = RyftQueryOptions(List("*.pcrime"), 0, 0)

  @Test def testSimpleQuery() {
    val query = SimpleQuery("query0")

    val queryString = "((RAW_TEXT%20CONTAINS%20%22query0%22))"

    val ryftQuery = RyftQueryHelper.queryAsString(query, queryOptions)
    assertEquals(queryString, ryftQuery._1)
  }

  @Test def testSimpleQueryComplex() {
    val query = SimpleQuery(List("query0","query1","query2"))

    val queryString = "((RAW_TEXT%20CONTAINS%20%22query0%22)OR(RAW_TEXT%20CONTAINS%20%22query1%22)OR" +
      "(RAW_TEXT%20CONTAINS%20%22query2%22))"

    val ryftQuery = RyftQueryHelper.queryAsString(query, queryOptions)
    assertEquals(queryString, ryftQuery._1)
  }

  @Test def testRyftRecordQuery() {
    val query =
      RecordQuery(record, contains, "VEHICLE")
        .and(recordField("date"), contains, "04/15/2015")

    val queryString = "((RECORD%20CONTAINS%20%22VEHICLE%22)AND" +
      "(RECORD.date%20CONTAINS%20%2204/15/2015%22))"

    val ryftQuery = RyftQueryHelper.queryAsString(query, queryOptions)
    assertEquals(queryString, ryftQuery._1)
  }

  @Test def testRyftRecordQueryComplex() {
    val query =
      RecordQuery(
        RecordQuery(recordField("desc"), contains, "VEHICLE")
          .or(recordField("desc"), contains, "BIKE")
          .or(recordField("desc"), contains, "MOTO"))
        .and(RecordQuery(recordField("date"), contains, "04/15/2015"))

    val queryString = "(((RECORD.desc%20CONTAINS%20%22VEHICLE%22)OR" +
      "(RECORD.desc%20CONTAINS%20%22BIKE%22)OR" +
      "(RECORD.desc%20CONTAINS%20%22MOTO%22))AND" +
      "((RECORD.date%20CONTAINS%20%2204/15/2015%22)))"

    val ryftQuery = RyftQueryHelper.queryAsString(query, queryOptions)
    assertEquals(queryString, ryftQuery._1)
  }
}

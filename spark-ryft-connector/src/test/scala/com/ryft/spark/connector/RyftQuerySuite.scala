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
import com.ryft.spark.connector.query.{RecordQuery, SimpleQuery}
import com.ryft.spark.connector.util.RyftQueryHelper
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Assert._

class RyftQuerySuite extends JUnitSuite {

  val queryOptions = RyftQueryOptions("*.pcrime", structured = false)

  @Test def testSimpleQuery() {
    val query = SimpleQuery("query0")

    val queryS = s"""(RAW_TEXT CONTAINS "query0")"""

    val ryftQuery = RyftQueryHelper.queryToString(query)
    assertEquals(queryS, ryftQuery)
  }

  @Test def testSimpleQueryComplex() {
    val query = SimpleQuery(List("query0","query1","query2"))

    val queryString = s"""((RAW_TEXT CONTAINS "query0")OR(RAW_TEXT CONTAINS "query1")OR""" +
      s"""(RAW_TEXT CONTAINS "query2"))"""

    val ryftQuery = RyftQueryHelper.queryToString(query)
    assertEquals(queryString, ryftQuery)
  }

  @Test def testSingleRyftRecordQuery() {
    val query = RecordQuery(record, contains, "VEHICLE")

    val queryString = s"""(RECORD CONTAINS "VEHICLE")"""

    val ryftQuery = RyftQueryHelper.queryToString(query)
    assertEquals(queryString, ryftQuery)
  }

  @Test def testRyftRecordQuery() {
    val query =
      RecordQuery(record, contains, "VEHICLE")
        .and(recordField("date"), contains, "04/15/2015")

    val queryString = s"""((RECORD.date CONTAINS "04/15/2015")AND"""+
      s"""(RECORD CONTAINS "VEHICLE"))"""

    val ryftQuery = RyftQueryHelper.queryToString(query)
    assertEquals(queryString, ryftQuery)
  }

  @Test def testRyftRecordQueryComplex() {
    val query =
      RecordQuery(
        RecordQuery(recordField("desc"), contains, "VEHICLE")
          .or(recordField("desc"), contains, "BIKE")
          .or(recordField("desc"), contains, "MOTO"))
        .and(RecordQuery(recordField("date"), contains, "04/15/2015"))

    val queryString = s"""((RECORD.date CONTAINS "04/15/2015")AND"""+
      s"""((RECORD.desc CONTAINS "MOTO")OR"""+
      s"""(RECORD.desc CONTAINS "BIKE")OR"""+
      s"""(RECORD.desc CONTAINS "VEHICLE")))"""

    val ryftQuery = RyftQueryHelper.queryToString(query)
    assertEquals(queryString, ryftQuery)
  }
}

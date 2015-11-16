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

import com.ryft.spark.connector.domain.{equalTo, recordField, contains, record}
import com.ryft.spark.connector.query.filter.{Or, EqualTo, Contains}
import com.ryft.spark.connector.query.RecordQuery
import org.junit.Test
import org.scalatest.junit.JUnitSuite
import org.junit.Assert._

class RecordQuerySuite extends JUnitSuite {

  @Test def simpleRecordQuery() = {
    val query = RecordQuery(record, contains, "VEHICLE")
    assertNotNull(query)
    assertNotNull(query.filters)
    assert(query.filters.size == 1)
    assertEquals(query.filters.head, Contains("RECORD", "VEHICLE"))
  }

  @Test def simpleRecordQueryAnd() = {
    val query = RecordQuery(record, contains, "VEHICLE")
      .and(recordField("field0"), equalTo, "value0")
    assertNotNull(query)
    assertNotNull(query.filters)
    assert(query.filters.size == 2)
    assertEquals(query.filters.head, EqualTo("RECORD.field0", "value0"))
    assertEquals(query.filters.tail.head, Contains("RECORD", "VEHICLE"))
  }

  @Test def simpleRecordQueryOr() = {
    val query = RecordQuery(record, contains, "VEHICLE")
      .or(recordField("field0"), equalTo, "value0")
    assertNotNull(query)
    assertNotNull(query.filters)
    assert(query.filters.size == 1)
    val left = query.filters.head.asInstanceOf[Or].left
    assertEquals(left, EqualTo("RECORD.field0", "value0"))
    val right = query.filters.head.asInstanceOf[Or].right
    assertEquals(right, Contains("RECORD", "VEHICLE"))
  }

  @Test def simpleRecordQueryMultipleOr() = {
    val query = RecordQuery(recordField("field0"), contains, "value0")
      .or(recordField("field1"), contains, "value1")
      .or(recordField("field2"), contains, "value2")

    assertNotNull(query)
    assertNotNull(query.filters)
    assert(query.filters.size == 1)
    val left = query.filters.head.asInstanceOf[Or].left
    assertEquals(left, Contains("RECORD.field2", "value2"))
    val right = query.filters.head.asInstanceOf[Or].right
    val value1 = right.asInstanceOf[Or].left
    val value2 = right.asInstanceOf[Or].right
    assertEquals(value1, Contains("RECORD.field1", "value1"))
    assertEquals(value2, Contains("RECORD.field0", "value0"))
  }

  @Test def complexRecordQuery() = {
    val query = RecordQuery(
      RecordQuery(recordField("desc"), contains, "VEHICLE")
        .or(recordField("desc"), contains, "BIKE")
        .or(recordField("desc"), contains, "MOTO"))
      .and(RecordQuery(recordField("date"), contains, "04/15/2015")
        .or(recordField("date"), contains, "04/14/2015"))

    val left = query.filters.head.asInstanceOf[Or]
    assertEquals(left.left, Contains("RECORD.date", "04/14/2015"))
    assertEquals(left.right, Contains("RECORD.date", "04/15/2015"))

    val right = query.filters.tail.head.asInstanceOf[Or]
    assertEquals(right.left, Contains("RECORD.desc", "MOTO"))
    assertEquals(right.right.asInstanceOf[Or].left, Contains("RECORD.desc", "BIKE"))
    assertEquals(right.right.asInstanceOf[Or].right, Contains("RECORD.desc", "VEHICLE"))
  }
}

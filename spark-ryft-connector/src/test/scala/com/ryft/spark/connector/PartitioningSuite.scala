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

import com.ryft.spark.connector.domain.{recordField, contains, record}
import com.ryft.spark.connector.query.RecordQuery
import com.ryft.spark.connector.util.RyftPartitioner
import org.junit.Test
import org.scalatest.junit.JUnitSuite
import org.junit.Assert._

class PartitioningSuite extends JUnitSuite {
  private val A2M_PARTITION = "http://52.20.99.136:8765"
  private val N2Z_DIGITS_PARTITION = "http://52.20.99.136:9000"

  //FIXME: Need to create test after finishing with partitioning improvements

//  @Test def n2zRecordQuery() = {
//    val query = RecordQuery(record, contains, "VEHICLE")
//    val partitions = RyftPartitioner.forRyftQuery(query, RyftPartitioner.byFirstLetter())
//    assertNotNull(partitions)
//    assert(partitions.size == 1)
//    assertEquals(N2Z_DIGITS_PARTITION, partitions.head)
//  }
//
//  @Test def a2mRecordQuery() = {
//    val query = RecordQuery(record, contains, "BIKE")
//    val partitions = RyftPartitioner.forRyftQuery(query, RyftPartitioner.byFirstLetter)
//    assertNotNull(partitions)
//    assert(partitions.size == 1)
//    assertEquals(A2M_PARTITION, partitions.head)
//  }
//
//  @Test def digitsRecordQuery() = {
//    val query = RecordQuery(record, contains, "2 VEHICLES")
//    val partitions = RyftPartitioner.forRyftQuery(query, RyftPartitioner.byFirstLetter)
//    assertNotNull(partitions)
//    assert(partitions.size == 1)
//    assertEquals(N2Z_DIGITS_PARTITION, partitions.head)
//  }
//
//  @Test def complexRecordQuery() = {
//    val query =
//      RecordQuery(
//        RecordQuery(record, contains, "VEHICLE")
//          .and(record, contains, "BIKE"))
//      .or(record, contains, "2 CARS")
//
//    val partitions = RyftPartitioner.forRyftQuery(query, RyftPartitioner.byFirstLetter)
//    assertNotNull(partitions)
//    assert(partitions.size == 2)
//    assert(partitions.contains(A2M_PARTITION))
//    assert(partitions.contains(N2Z_DIGITS_PARTITION))
//  }
//
//  @Test def complexRecordFieldQuery() = {
//    val query =
//      RecordQuery(
//        RecordQuery(recordField("field0"), contains, "VEHICLE")
//          .and(recordField("field1"), contains, "BIKE"))
//        .or(recordField("field2"), contains, "2 CARS")
//
//    val partitions = RyftPartitioner.forRyftQuery(query, RyftPartitioner.byFirstLetter,
//      Map("field1" -> "BIKE"))
//    assertNotNull(partitions)
//    assert(partitions.size == 1)
//    assert(partitions.contains(A2M_PARTITION))
//  }
//
//  @Test def complexRecordFieldsQuery() = {
//    val query =
//      RecordQuery(
//        RecordQuery(recordField("field0"), contains, "VEHICLE")
//          .and(recordField("field1"), contains, "BIKE"))
//        .or(recordField("field2"), contains, "2 CARS")
//
//    val partitions = RyftPartitioner.forRyftQuery(query, RyftPartitioner.byFirstLetter,
//      Map("field0" -> "VEHICLE",  "field1" -> "BIKE"))
//    assertNotNull(partitions)
//    assert(partitions.size == 2)
//    assert(partitions.contains(A2M_PARTITION))
//    assert(partitions.contains(N2Z_DIGITS_PARTITION))
//  }
}

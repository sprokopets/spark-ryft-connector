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

import com.ryft.spark.connector.util.FilterConverter
import org.apache.spark.sql.sources.{Or, And, EqualTo}
import org.junit.Test
import org.scalatest.junit.JUnitSuite
import org.junit.Assert._
import com.ryft.spark.connector.query._

class FilterConverterSuite extends JUnitSuite {
  @Test def simpleFilterConversion() = {
    val f = EqualTo("attribute", "value")
    val rq = FilterConverter.filtersToRecordQuery(Array(f))
    assertNotNull(rq)
    assertNotNull(rq.filters)
    assert(rq.filters.size == 1)
    assertEquals(rq.filters.head, filter.EqualTo("RECORD.attribute", "value"))
  }

  @Test def simpleOrFilterConversion() = {
    val orF = Or(EqualTo("attribute0", "value0"), EqualTo("attribute1", "value1"))
    val rq = FilterConverter.filtersToRecordQuery(Array(orF))
    assertNotNull(rq)
    assertNotNull(rq.filters)
    assert(rq.filters.size == 1)
    val ryftOr = rq.filters.head.asInstanceOf[filter.Or]
    assertEquals(ryftOr.left, filter.EqualTo("RECORD.attribute0", "value0"))
    assertEquals(ryftOr.right, filter.EqualTo("RECORD.attribute1", "value1"))
  }

  @Test def simpleAndFilterConversion() = {
    val orF = And(EqualTo("attribute0", "value0"), EqualTo("attribute1", "value1"))
    val rq = FilterConverter.filtersToRecordQuery(Array(orF))
    assertNotNull(rq)
    assertNotNull(rq.filters)
    assert(rq.filters.size == 1)
    val ryftAnd = rq.filters.head.asInstanceOf[filter.And]
    assertEquals(ryftAnd.left, filter.EqualTo("RECORD.attribute0", "value0"))
    assertEquals(ryftAnd.right, filter.EqualTo("RECORD.attribute1", "value1"))
  }
}

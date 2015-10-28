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

package com.ryft.spark.connector.util

import com.ryft.spark.connector.query._
import com.ryft.spark.connector.query.RecordQuery
import org.apache.spark.sql.sources._

import scala.annotation.tailrec

object FilterConverter {
  @tailrec
  def filtersToRecordQuery(filters: Array[Filter],
      acc: RecordQuery = RecordQuery()): RecordQuery = {
    if (filters.isEmpty) acc
    else filtersToRecordQuery(filters.tail, acc.and(filterToRecordQuery(filters.head)))
  }

  private def filterToRecordQuery(filter: Filter): RecordQuery = RecordQuery(toRyftFilter(filter))

  //TODO: try to implement it tailrec
  //This method is not tail recursive but  large depth isn't assumed here
  private def toRyftFilter(f: Filter): filter.Filter = f match {
    case Not(filter) => not(filter)
    case EqualTo(attr, v) => filter.EqualTo("RECORD."+attr,v.toString) //TODO: check if we should not use String everywhere
    case StringContains(attr, v) => filter.Contains("RECORD."+attr, v)
    case StringStartsWith(attr, v) => filter.Contains("RECORD."+attr, v)
    case StringEndsWith(attr, v) => filter.Contains("RECORD."+attr, v)

    case Or(left, right) => filter.Or(toRyftFilter(left), toRyftFilter(right))
    case And(left, right) => filter.And(toRyftFilter(left), toRyftFilter(right))
    case _ =>
      println("error: "+ f)
      throw new RuntimeException //TODO: exception + message
  }

  private def not(f: Filter): filter.Filter = f match {
    case EqualTo(attr, v) => filter.NotEqualTo("RECORD."+attr,v.toString) //TODO: check if we should not use String everywhere
    case StringContains(attr, v) => filter.NotContains("RECORD."+attr, v)
    case StringStartsWith(attr, v) => filter.NotContains("RECORD."+attr, v)
    case StringEndsWith(attr, v) => filter.NotContains("RECORD."+attr, v)
    case _ =>
      println("error: "+ f)
      throw new RuntimeException //TODO: exception + message
  }
}

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

import com.fasterxml.jackson.core.{JsonParser, JsonToken}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SimpleJsonParser {

  def hasNext(parser: JsonParser) = {
    if (parser.getCurrentToken == null) parser.nextToken()

    parser.getCurrentToken != null &&
      parser.getCurrentToken != JsonToken.END_ARRAY
  }

  def parseJson(parser: JsonParser) = {
    if (parser.getCurrentToken == null) parser.nextToken()

    parser.getCurrentToken match {
      case JsonToken.START_OBJECT =>
        parseJsonObj(parser)
      case JsonToken.START_ARRAY  =>
        parser.nextToken() //TODO: skip
        if (parser.getCurrentToken == null) parser.nextToken()
        if (parser.getCurrentToken != JsonToken.END_ARRAY) parseJsonObj(parser)
      case _                      => //TODO: something wrong, handle case
    }
  }

//  @tailrec TODO: make it tailrec
  def parseJsonObj(parser: JsonParser): Map[String, Any] = {
    if (parser.getCurrentToken == null) parser.nextToken()

    assert(parser.getCurrentToken == JsonToken.START_OBJECT)
    parser.nextToken()

    val map = mutable.Map.empty[String, Any]
    while (parser.getCurrentToken != JsonToken.END_OBJECT) {
      assert(parser.getCurrentToken == JsonToken.FIELD_NAME)
      val fieldName = parser.getCurrentName
      parser.nextToken()

      val fieldValue = parser.getCurrentToken match {
        case JsonToken.VALUE_STRING   =>
          val fieldValue = parser.getValueAsString
          parser.nextToken()
          fieldValue

        case JsonToken.VALUE_NUMBER_INT =>
          val fieldValue = parser.getValueAsString
          parser.nextToken()
          fieldValue

        case JsonToken.VALUE_NULL =>
          val fieldValue = parser.getValueAsString
          parser.nextToken()
          fieldValue

        case JsonToken.START_OBJECT   =>
          parseJsonObj(parser)
        case JsonToken.START_ARRAY    =>
          parseJsonArray(parser)
        case _ => println("!error") //TODO: need normal reaction, check other cases?
      }
      map += (fieldName -> fieldValue)
    }
    parser.nextToken()

    map.toMap
  }

  def parseJsonArray(parser: JsonParser) = {
    if (parser.getCurrentToken == null) parser.nextToken()

    assert(parser.getCurrentToken == JsonToken.START_ARRAY)
    parser.nextToken()

    val buffer = new ListBuffer[Map[String,Any]]
    while (parser.getCurrentToken != JsonToken.END_ARRAY) {
      assert(parser.getCurrentToken == JsonToken.START_OBJECT)
      buffer += parseJsonObj(parser)
    }
    parser.nextToken() //read out JsonToken.END_ARRAY
    buffer.toList
  }
}

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

import com.fasterxml.jackson.core.{JsonToken, JsonParser}
import com.ryft.spark.connector.RyftSparkException
import org.apache.spark.Logging

import scala.annotation.tailrec
import scala.collection.mutable

object SimpleJsonParser extends Logging {

  def parseJson(parser: JsonParser) = {
    parser.nextToken match {
      case JsonToken.START_OBJECT =>
        parserObjAcc(parser, mutable.Map.empty[String,Any])
      case JsonToken.FIELD_NAME   => //FIXME: workaround, somehow we lost START_OBJECT
        parserObjAcc(parser, mutable.Map.empty[String,Any])
      case JsonToken.START_ARRAY  =>
        parseArrayAcc(parser, mutable.ListBuffer.empty[Map[String,Any]])
      case null                   =>
        logInfo("Finished parsing stream")
      case _                      =>
        val msg = s"Unable to parse current token: ${parser.getCurrentToken}"
        logWarning(msg)
        throw new RyftSparkException(msg)
    }
  }

  @tailrec
  private def parserObjAcc(parser: JsonParser, acc: mutable.Map[String,Any]): Map[String, Any]  = {
    if (parser.getCurrentToken == JsonToken.START_OBJECT) parser.nextToken

    if (parser.getCurrentToken == JsonToken.END_OBJECT) {
      parser.nextToken
      acc.toMap
    } else {
      val fieldName = parser.getCurrentName
      parser.nextToken

      if (parser.getCurrentToken == JsonToken.START_OBJECT) {
        val nestedObj = parseNestedObj(parser)
        acc.put(fieldName, nestedObj)
        parserObjAcc(parser, acc)
      } else if (parser.getCurrentToken == JsonToken.START_ARRAY) {
        val arrayVal = parseArrayAcc(parser, mutable.ListBuffer.empty[Map[String,Any]])
        acc.put(fieldName, arrayVal)
        parserObjAcc(parser, acc)
      } else {
        val filedValue = parser.getValueAsString
        parser.nextToken

        acc.put(fieldName, filedValue)
        parserObjAcc(parser, acc)
      }
    }
  }

  @tailrec
  private def parseArrayAcc(parser: JsonParser, acc: mutable.ListBuffer[Map[String, Any]]): List[Map[String, Any]] = {
    if (parser.getCurrentToken == JsonToken.START_ARRAY) parser.nextToken

    if (parser.getCurrentToken == JsonToken.END_ARRAY) {
      parser.nextToken
      acc.toList
    } else {
      acc += parserObjAcc(parser, mutable.Map.empty[String,Any])
      parseArrayAcc(parser, acc)
    }
  }

  private def parseNestedObj(parser: JsonParser) = {
    parserObjAcc(parser, mutable.Map.empty[String,Any])
  }
}

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

import java.io.ByteArrayInputStream

import com.fasterxml.jackson.databind.ObjectMapper
import com.ryft.spark.connector.util.SimpleJsonParser
import org.scalatest.FunSuite

class JsonSuite extends FunSuite {
  val simpleJsonObj =
    "{" +
    "\"field1\": \"value1\"," +
    "\"field2\": \"value2\"" +
    "}"

  val simpleJsonArray =
    "[" +
    "{" +
    "\"field1\": \"value1\"," +
    "\"field2\": \"value2\"" +
    "},"+
    "{" +
    "\"field11\": \"value11\"," +
    "\"field21\": \"value21\"" +
    "}"+
    "]"

  val complexJsonObj =
    "{" +
      "\"field1\": \"value1\"," +
      "\"field2\": {" +
        "\"field11\" : \"field22\"" +
      "}," +
      "\"field3\": \"value3\"" +
    "}"

  val complexJsonArray =
    "{" +
      "\"field1\": \"value1\"," +
      "\"field2\": [" +
        "{" +
          "\"field11\" : \"value11\"," +
          "\"field12\" : \"value12\"" +
        "}," +
        "{" +
        "\"field21\" : \"value22\"" +
        "}" +
      "]," +
      "\"field3\": \"value3\"" +
    "}"

  val complexJson =
    "[" +
      complexJsonArray +
      "," +
      complexJsonObj +
    "]"

  test("test parse simple json object") {
    val is = new ByteArrayInputStream(simpleJsonObj.getBytes)
    val lines = scala.io.Source.fromInputStream(is).getLines()
    val parser = new ObjectMapper().getFactory.createParser(lines.mkString("\n"))

    val jsonMap = SimpleJsonParser.parseJson(parser).asInstanceOf[Map[String,String]]
    assert(jsonMap != null)
    assert(jsonMap.nonEmpty)
    assert(jsonMap("field1").equals("value1"))
    assert(jsonMap("field2").equals("value2"))
  }

  test("test parse simple array json objects") {
    val is = new ByteArrayInputStream(simpleJsonArray.getBytes)
    val lines = scala.io.Source.fromInputStream(is).getLines()
    val parser = new ObjectMapper().getFactory.createParser(lines.mkString("\n"))

    val jsonList = SimpleJsonParser.parseJson(parser).asInstanceOf[List[Map[String,String]]]
    assert(jsonList != null)
    assert(jsonList.nonEmpty)

    val firstMap = jsonList.head
    assert(firstMap != null)
    assert(firstMap("field1").equals("value1"))
    assert(firstMap("field2").equals("value2"))

    val secondMap = jsonList.tail.head
    assert(secondMap != null)
    assert(secondMap.nonEmpty)
    assert(secondMap("field11").equals("value11"))
    assert(secondMap("field21").equals("value21"))
  }

  test("test parse complex json object") {
    val is = new ByteArrayInputStream(complexJsonObj.getBytes)
    val lines = scala.io.Source.fromInputStream(is).getLines()
    val parser = new ObjectMapper().getFactory.createParser(lines.mkString("\n"))

    val jsonMap = SimpleJsonParser.parseJson(parser).asInstanceOf[Map[String,String]]
    assert(jsonMap != null)
    assert(jsonMap.nonEmpty)
    assert(jsonMap("field1").equals("value1"))
    assert(jsonMap("field3").equals("value3"))

    val map = jsonMap("field2").asInstanceOf[Map[String, Any]]
    assert(map != null)
    assert(map.nonEmpty)
    assert(map("field11").equals("field22"))
  }

  test("test parse complex json array") {
    val is = new ByteArrayInputStream(complexJsonArray.getBytes)
    val lines = scala.io.Source.fromInputStream(is).getLines()
    val parser = new ObjectMapper().getFactory.createParser(lines.mkString("\n"))

    val jsonMap = SimpleJsonParser.parseJson(parser).asInstanceOf[Map[String,Any]]
    assert(jsonMap != null)
    assert(jsonMap.nonEmpty)
    assert(jsonMap("field1").equals("value1"))
    assert(jsonMap("field3").equals("value3"))

    val list = jsonMap("field2").asInstanceOf[List[Map[String, String]]]
    assert(list != null)
    assert(list.nonEmpty)
    assert(list.size == 2)

    val first = list.head
    assert(first != null)
    assert(first.nonEmpty)
    assert(first.size == 2)
    assert(first("field12").equals("value12"))
    assert(first("field11").equals("value11"))

    val second = list.tail.head
    assert(second != null)
    assert(second.nonEmpty)
    assert(second.size == 1)
    assert(second("field21").equals("value22"))
  }

  test("test parse complex json") {
    val is = new ByteArrayInputStream(complexJson.getBytes)
    val lines = scala.io.Source.fromInputStream(is).getLines()
    val parser = new ObjectMapper().getFactory.createParser(lines.mkString("\n"))

    val jsonMap = SimpleJsonParser.parseJson(parser).asInstanceOf[List[Any]]
    assert(jsonMap != null)

    val firstElem = jsonMap.head.asInstanceOf[Map[String, Any]]
    assert(firstElem != null)

    assert(firstElem("field1").equals("value1"))
    assert(firstElem("field3").equals("value3"))

    val list = firstElem("field2").asInstanceOf[List[Map[String, String]]]
    assert(list != null)
    assert(list.nonEmpty)
    assert(list.size == 2)

    val first = list.head
    assert(first != null)
    assert(first.nonEmpty)
    assert(first.size == 2)
    assert(first("field12").equals("value12"))
    assert(first("field11").equals("value11"))

    val second = list.tail.head
    assert(second != null)
    assert(second.nonEmpty)
    assert(second.size == 1)
    assert(second("field21").equals("value22"))


    val secondElem = jsonMap.tail.head.asInstanceOf[Map[String, Any]]
    assert(secondElem != null)
    assert(secondElem.nonEmpty)
    assert(secondElem("field1").equals("value1"))
    assert(secondElem("field3").equals("value3"))

    val map = secondElem("field2").asInstanceOf[Map[String, Any]]
    assert(map != null)
    assert(map.nonEmpty)
    assert(map("field11").equals("field22"))
  }
}

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

package com.ryft.spark.connector.rest

import java.net.{HttpURLConnection, URL}

import com.ryft.spark.connector.exception.RyftRestException
import org.apache.commons.io.IOUtils
import org.apache.spark.Logging

import scala.util.{Failure, Success, Try}

/**
 * Represents simple connection to Ryft Rest service.
 * By default add headers:
 *  Accept: application/msgpack
 *  Transfer-Encoding: chunked
 *
 * @param url Ryft Rest query
 */
class RyftRestConnection(url: String,
    requestProperties: Map[String, String] = Map.empty[String,String])
  extends Logging {

  private val connection = (Try(new URL(url).openConnection()) match {
    case Success(urlConnection) => urlConnection
    case Failure(ex) =>
      val msg = s"Unable to open url connection: $url"
      logWarning(msg)
      throw new RyftRestException(msg, ex)
  }).asInstanceOf[HttpURLConnection]

  requestProperties.foreach { case (key, value) =>
    logDebug(s"Used request header: '$key: $value'")
    connection.setRequestProperty(key, value)
  }

  def getInputStream = Try(connection.getInputStream) match {
    case Success(is) => is
    case Failure(ex) =>
      val msg = s"Unable to get InputStream from url connection: $url"
      logWarning(msg)
      throw new RyftRestException(msg, ex)
  }

  def result = {
    if (connection.getResponseCode == 200) {
      val in = connection.getInputStream
      val body = IOUtils.toString(in, "UTF-8")
      body
    } else {
      val errorStream = IOUtils.toString(connection.getErrorStream, "UTF-8")
      val msg = s"Ryft REST connection failed.\n" +
        s"URL: ${connection.getURL}\n" +
        s"Error Code: ${connection.getResponseCode}\n" +
        s"Error Message: $errorStream"
      logWarning(msg)
      throw new RyftRestException(msg)
    }
  }
}

object RyftRestConnection {
  def apply(url: String, requestProperties: Map[String, String]) =
    new RyftRestConnection(url, requestProperties)
}

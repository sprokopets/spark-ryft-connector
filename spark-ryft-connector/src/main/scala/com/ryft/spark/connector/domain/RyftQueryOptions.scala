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

package com.ryft.spark.connector.domain

import scala.language.postfixOps

/**
 * Represents Meta information used for making request to Ryft
 *
 * @param files Files to search
 * @param surrounding Width when generating results. For example, a value of 2 means that 2
 *                    characters before and after a search match will be included with data result
 * @param fuzziness Specify the fuzzy search distance [0..255]
 */
case class RyftQueryOptions(files: List[String],
    surrounding: Option[Int],
    fuzziness: Option[Byte],
    fields: Option[List[String]],
    ryftNodes: Option[Int],
    structured: Boolean)

object RyftQueryOptions {
  def apply(file: String, structured: Boolean) =
    new RyftQueryOptions(List(file), None, None, None, None, structured)


  def apply(files: List[String], structured: Boolean) =
    new RyftQueryOptions(files, None, None, None, None, structured)


  def apply(file: String, surrounding: Int, fuzziness: Byte) = {
    new RyftQueryOptions(List(file), Some(surrounding), Some(fuzziness), None, None, false)
  }

  def apply(files: List[String], surrounding: Int, fuzziness: Byte) = {
    new RyftQueryOptions(files, Some(surrounding), Some(fuzziness), None, None, false)
  }

  def apply(file: String, fields: List[String]) = {
    new RyftQueryOptions(List(file), None, None, Some(fields), None, true)
  }

  def apply(files: List[String], fields: List[String]) = {
    new RyftQueryOptions(files, None, None, Some(fields), None, true)
  }
}

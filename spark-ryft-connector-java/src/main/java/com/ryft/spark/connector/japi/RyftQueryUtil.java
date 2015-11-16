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

package com.ryft.spark.connector.japi;

import com.ryft.spark.connector.domain.InputSpecifier;
import com.ryft.spark.connector.domain.RelationalOperator;
import com.ryft.spark.connector.domain.contains$;
import com.ryft.spark.connector.domain.recordField;
import com.ryft.spark.connector.query.RecordQuery;
import com.ryft.spark.connector.query.RecordQuery$;
import com.ryft.spark.connector.query.SimpleQuery;
import com.ryft.spark.connector.query.SimpleQuery$;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class RyftQueryUtil {

    public static SimpleQuery toSimpleQuery(String query) {
        return SimpleQuery$.MODULE$.apply(query);
    }

    public static List<SimpleQuery> toSimpleQueries(String ... queries) {
        return Arrays.asList(queries)
                .stream()
                .map(SimpleQuery$.MODULE$::apply)
                .collect(Collectors.toList());
    }

    public static RecordQuery toRecordQuery(InputSpecifier is, RelationalOperator ro, String value) {
        return RecordQuery$.MODULE$.apply(is, ro, value);
    }
}

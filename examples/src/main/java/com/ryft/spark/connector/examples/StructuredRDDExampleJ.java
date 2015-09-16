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

package com.ryft.spark.connector.examples;

import com.ryft.spark.connector.RyftQueryBuilder;
import com.ryft.spark.connector.domain.RyftQueryOptions;
import com.ryft.spark.connector.domain.query.RyftRecordQuery;
import com.ryft.spark.connector.japi.RyftJavaUtil;
import com.ryft.spark.connector.japi.SparkContextJavaFunctions;
import com.ryft.spark.connector.japi.rdd.RyftJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import com.ryft.spark.connector.domain.query.*;
import com.ryft.spark.connector.domain.query.contains$;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.HashMap;

@SuppressWarnings("unchecked")
public class StructuredRDDExampleJ {
    private static final Logger logger = LoggerFactory.getLogger(StructuredRDDExampleJ.class);

    public static void main(String[] args) {
        final SparkConf sparkConf = new SparkConf()
                .setAppName("SimplePairRDDExample")
                .setMaster("local[2]")
                .set("spark.locality.wait", "120s")
                .set("spark.locality.wait.node", "120s");
        final byte fuzziness = 0;
        final int surrounding = 0;

        final SparkContext sc = new SparkContext(sparkConf);
        final SparkContextJavaFunctions javaFunctions = RyftJavaUtil.javaFunctions(sc);

        final RyftRecordQuery query = new RyftQueryBuilder(new recordField("date"), contains$.MODULE$, "04/15/2015")
            .and(new recordField("desc"), contains$.MODULE$, "VEHICLE")
        .or(new recordField("date"), contains$.MODULE$, "04/14/2015")
            .and(new recordField("desc"), contains$.MODULE$, "VEHICLE")
        .build();

        final RyftJavaRDD<HashMap.HashTrieMap<String,String>> ryftRDDStructured =
                javaFunctions.ryftRDDStructured(query,
                        new RyftQueryOptions("*.pcrime", surrounding, fuzziness));

        final JavaPairRDD<Option<String>, Integer> counts =
                ryftRDDStructured.mapToPair(map -> new Tuple2<>(map.get("LocationDescription"), 1))
                .reduceByKey((a, b) -> a + b);

        counts.foreach(tuple -> {
            logger.info("key: {} value: {}", tuple._1().get(), tuple._2());
        });
    }
}

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

import com.ryft.spark.connector.domain.RyftQueryOptions;
import com.ryft.spark.connector.japi.RyftJavaUtil;
import com.ryft.spark.connector.japi.RyftQueryUtil;
import com.ryft.spark.connector.japi.SparkContextJavaFunctions;
import com.ryft.spark.connector.japi.rdd.RyftJavaRDD;
import com.ryft.spark.connector.query.SimpleQuery;
import com.ryft.spark.connector.query.SimpleQuery$;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleRDDExampleJ {
    private static final Logger logger = LoggerFactory.getLogger("SimpleRDDExampleJ");

    public static void main(String[] args) {
        final SparkConf sparkConf = new SparkConf()
                .setAppName("SimpleRDDExampleJ")
                .setMaster("local[2]")
                .set("spark.ryft.rest.url", "http://52.20.99.136:9000");

        final SparkContext sc = new SparkContext(sparkConf);
        final SparkContextJavaFunctions javaFunctions = RyftJavaUtil.javaFunctions(sc);
        final byte fuzziness = 0;
        final int surrounding = 10;
        final SimpleQuery query = RyftQueryUtil.toSimpleQuery("Jones");

        final RyftJavaRDD rdd = javaFunctions.ryftRDD(query,
                RyftQueryOptions.apply("passengers.txt", surrounding, fuzziness),
                RyftJavaUtil.ryftQueryToEmptyList,
                RyftJavaUtil.stringToEmptySet);

        logger.info("count: {}", rdd.count());
    }
}

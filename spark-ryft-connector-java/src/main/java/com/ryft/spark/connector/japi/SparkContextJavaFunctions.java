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

import com.ryft.spark.connector.SparkContextFunctions;
import com.ryft.spark.connector.domain.RyftData;
import com.ryft.spark.connector.domain.RyftQueryOptions;
import com.ryft.spark.connector.query.RecordQuery;
import com.ryft.spark.connector.query.RyftQuery;
import com.ryft.spark.connector.query.SimpleQuery;
import com.ryft.spark.connector.japi.rdd.RyftJavaRDD;
import com.ryft.spark.connector.japi.rdd.RyftPairJavaRDD;
import com.ryft.spark.connector.rdd.RyftPairRDD;
import com.ryft.spark.connector.rdd.RyftRDD;
import com.ryft.spark.connector.util.JavaApiHelper;
import org.apache.spark.SparkContext;
import scala.Function1;
import scala.Tuple2;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.List;
import scala.collection.immutable.Map;
import scala.collection.immutable.Set;
import scala.reflect.ClassTag;

import static com.ryft.spark.connector.util.JavaApiHelper.getClassTag;

@SuppressWarnings({"UnusedDeclaration","unchecked"})
public class SparkContextJavaFunctions {
    private final SparkContext sparkContext;
    private final SparkContextFunctions sparkContextFunctions;

    SparkContextJavaFunctions(SparkContext sparkContext) {
        this.sparkContext = sparkContext;
        this.sparkContextFunctions = new SparkContextFunctions(sparkContext);
    }

    public <T> RyftJavaRDD<T> toJavaRDD(RyftRDD<T> rdd, Class<T> targetClass) {
        final ClassTag classTag = getClassTag(targetClass);
        return new RyftJavaRDD<>(rdd, classTag);
    }

    public <T> RyftJavaRDD<T> ryftRDD(RyftQuery ryftQuery,
                                      RyftQueryOptions queryOptions,
                                      Function1<RyftQuery, List<String>> choosePartitions,
                                      Function1<String, Set<String>> preferredLocations) {
        final RyftRDD ryftRDD = sparkContextFunctions.ryftRDD(JavaApiHelper.toScalaSeq(ryftQuery),
                queryOptions, choosePartitions, preferredLocations);
        return toJavaRDD(ryftRDD, RyftData.class);
    }

    public RyftJavaRDD ryftRDDStructured(RecordQuery ryftQuery,
                                         RyftQueryOptions queryOptions,
                                         Function1<RyftQuery, List<String>> choosePartitions,
                                         Function1<String, Set<String>> preferredLocations) {
        final RyftRDD ryftRDD =  sparkContextFunctions.ryftRDD(JavaApiHelper.toScalaSeq(ryftQuery),
                queryOptions, choosePartitions, preferredLocations);
        return toJavaRDD(ryftRDD, HashMap.HashTrieMap.class);
    }

    public <T> RyftPairJavaRDD<Tuple2<String, T>> ryftPairJavaRDD(RyftQuery ryftQuery,
                                                                  RyftQueryOptions queryOptions,
                                                                  Function1<RyftQuery, List<String>> choosePartitions,
                                                                  Function1<String, Set<String>> preferredLocations) {
        final RyftPairRDD ryftPairRDD = sparkContextFunctions.ryftPairRDD(JavaApiHelper.toScalaSeq(ryftQuery),
                queryOptions, choosePartitions, preferredLocations);
        return toPairJavaRDD(ryftPairRDD, RyftData.class);
    }

    public <T> RyftPairJavaRDD<Tuple2<String, T>> ryftPairJavaRDD(List<RyftQuery> ryftQuery,
                                                                  RyftQueryOptions queryOptions,
                                                                  Function1<RyftQuery, List<String>> choosePartitions,
                                                                  Function1<String, Set<String>> preferredLocations) {
        final RyftPairRDD ryftPairRDD = sparkContextFunctions.ryftPairRDD(ryftQuery,
                queryOptions, choosePartitions, preferredLocations);
        return toPairJavaRDD(ryftPairRDD, RyftData.class);
    }

    private <T> RyftPairJavaRDD<T> toPairJavaRDD(RyftPairRDD<T> rdd, Class<T> targetClass) {
        final ClassTag classTag = getClassTag(targetClass);
        return new RyftPairJavaRDD<>(rdd, classTag);
    }
}
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

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataFrameExampleJ {
    private static final Logger logger = LoggerFactory.getLogger(DataFrameExampleJ.class);

    public static void main(String[] args) {
        final SparkConf sparkConf = new SparkConf()
                .setAppName("SimplePairRDDExampleJ")
                .setMaster("local[2]");

        final SparkContext sc = new SparkContext(sparkConf);
        final SQLContext sqlContext = new SQLContext(sc);

        final List<StructField> index = Arrays.asList(
            DataTypes.createStructField("file", DataTypes.IntegerType, true),
            DataTypes.createStructField("offset", DataTypes.IntegerType, true),
            DataTypes.createStructField("length", DataTypes.IntegerType, true),
            DataTypes.createStructField("fuzziness", DataTypes.IntegerType, true));

        final StructType schema = DataTypes.createStructType(
            Arrays.asList(DataTypes.createStructField("Arrest", DataTypes.StringType, true),
                DataTypes.createStructField("Beat", DataTypes.IntegerType, true),
                DataTypes.createStructField("Block", DataTypes.StringType, true),
                DataTypes.createStructField("CaseNumber", DataTypes.StringType, true),
                DataTypes.createStructField("CommunityArea", DataTypes.IntegerType, true),
                DataTypes.createStructField("Date", DataTypes.StringType, true),
                DataTypes.createStructField("Description", DataTypes.StringType, true),
                DataTypes.createStructField("Domestic", DataTypes.IntegerType, true),
                DataTypes.createStructField("FBICode", DataTypes.BooleanType, true),
                DataTypes.createStructField("ID", DataTypes.StringType, true),
                DataTypes.createStructField("IUCR", DataTypes.IntegerType, true),
                DataTypes.createStructField("Latitude", DataTypes.DoubleType, true),
                DataTypes.createStructField("Location", DataTypes.StringType, true),
                DataTypes.createStructField("LocationDescription", DataTypes.StringType, true),
                DataTypes.createStructField("Longitude", DataTypes.DoubleType, true),
                DataTypes.createStructField("PrimaryType", DataTypes.StringType, true),
                DataTypes.createStructField("UpdatedOn", DataTypes.StringType, true),
                DataTypes.createStructField("Ward", DataTypes.IntegerType, true),
                DataTypes.createStructField("XCoordinate", DataTypes.IntegerType, true),
                DataTypes.createStructField("YCoordinate", DataTypes.IntegerType, true),
                DataTypes.createStructField("Year", DataTypes.IntegerType, true),
                DataTypes.createStructField("_index", DataTypes.createStructType(index), true)));


        final DataFrame crimes = sqlContext.read()
            .format("com.ryft.spark.connector.sql")
            .schema(schema)
            .option("files", "*.pcrime")
            .load();

        crimes.registerTempTable("crimes");

        final DataFrame df = sqlContext.sql("" +
            "select Date, ID, Description, Arrest from crimes"
            + " where Description LIKE '%VEHICLE%'"
            + " AND (Date LIKE '%04/15/2015%' OR Date LIKE '%04/14/2015%' OR Date LIKE '%04/13/2015%')"
            + " ORDER BY Date");

        final Integer result = df.collect().length;
        logger.info("Result count: {}", result);
    }
}

package com.ryft.spark.connector.examples;

import com.ryft.spark.connector.domain.RyftMetaInfo;
import com.ryft.spark.connector.domain.query.SimpleRyftQuery;
import com.ryft.spark.connector.japi.RyftJavaUtil;
import com.ryft.spark.connector.japi.SparkContextJavaFunctions;
import com.ryft.spark.connector.japi.rdd.RyftPairJavaRDD;
import com.ryft.spark.connector.util.JavaApiHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.util.Collections;

public class SimplePairRDDExampleJ {
    private static final Logger logger = LoggerFactory.getLogger(SimplePairRDDExampleJ.class)

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("SimplePairRDDExample")
//                .setMaster("local[2]")
                .set("spark.locality.wait", "120s")
                .set("spark.locality.wait.node", "120s");

        final SparkContext sc = new SparkContext(sparkConf);
        final SparkContextJavaFunctions javaFunctions = RyftJavaUtil.javaFunctions(sc);
        final String[] query = {"john"};
        final String[] files = {"reddit/*"};
        final byte b = 0;

        final Seq<String> querySec = JavaApiHelper.<String>toScalaSeq(query);
        final Seq<String> filesSeq = JavaApiHelper.<String>toScalaSeq(files);

        final RyftPairJavaRDD rdd = javaFunctions.ryftPairJavaRDD(
                Collections.singletonList(new SimpleRyftQuery(querySec.toList())),
                new RyftMetaInfo(filesSeq.toList(), 10, b));
        logger.info("count: {}", rdd.count());
    }
}

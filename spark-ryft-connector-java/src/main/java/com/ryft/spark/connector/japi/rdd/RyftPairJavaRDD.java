package com.ryft.spark.connector.japi.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.reflect.ClassTag;

@SuppressWarnings({"unchecked", "UnusedDeclaration"})
public class RyftPairJavaRDD<T> extends JavaRDD<T> {
    public RyftPairJavaRDD(RDD<T> rdd, ClassTag<T> classTag) {
        super(rdd, classTag);
    }
}

package com.ryft.spark.connector.japi;

import org.apache.spark.SparkContext;

public class RyftJavaUtil {
    /**
     * A static factory method to create a {@link SparkContextJavaFunctions} based on an existing {@link
     * SparkContext} instance.
     */
    public static SparkContextJavaFunctions javaFunctions(SparkContext sparkContext) {
        return new SparkContextJavaFunctions(sparkContext);
    }
}

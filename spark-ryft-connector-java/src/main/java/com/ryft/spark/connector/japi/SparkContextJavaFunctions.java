package com.ryft.spark.connector.japi;

import com.ryft.spark.connector.SparkContextFunctions;
import com.ryft.spark.connector.domain.RyftData;
import com.ryft.spark.connector.domain.RyftMetaInfo;
import com.ryft.spark.connector.domain.query.SimpleRyftQuery;
import com.ryft.spark.connector.japi.rdd.RyftPairJavaRDD;
import com.ryft.spark.connector.rdd.RyftPairRDD;
import org.apache.spark.SparkContext;
import scala.Tuple2;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.util.List;

import static com.ryft.spark.connector.util.JavaApiHelper.getClassTag;
import static com.ryft.spark.connector.util.JavaApiHelper.toScalaSeq;

@SuppressWarnings({"UnusedDeclaration","unchecked"})
public class SparkContextJavaFunctions {
    private final SparkContext sparkContext;
    private final SparkContextFunctions sparkContextFunctions;

    SparkContextJavaFunctions(SparkContext sparkContext) {
        this.sparkContext = sparkContext;
        this.sparkContextFunctions = new SparkContextFunctions(sparkContext);
    }

    public <T> RyftPairJavaRDD<Tuple2<String, T>> toJavaRDD(RyftPairRDD<T> rdd, Class<T> targetClass) {
        final ClassTag classTag = getClassTag(targetClass);
        return new RyftPairJavaRDD<>(rdd, classTag);
    }

    public <T> RyftPairJavaRDD<Tuple2<String, T>> ryftPairJavaRDD(List<SimpleRyftQuery> ryftQueries,
                                                       RyftMetaInfo metaInfo) {
        final Seq seq = toScalaSeq(ryftQueries.toArray(new SimpleRyftQuery[ryftQueries.size()]));
        final RyftPairRDD ryftPairRDD = sparkContextFunctions.ryftPairRDD(seq.toList(),metaInfo);
        return toJavaRDD(ryftPairRDD, RyftData.class);
    }
}

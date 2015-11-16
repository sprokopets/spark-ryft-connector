package com.ryft.spark.connector.japi;

import com.ryft.spark.connector.domain.contains$;
import com.ryft.spark.connector.domain.equalTo$;
import com.ryft.spark.connector.domain.notContains$;
import com.ryft.spark.connector.domain.notEqualTo$;

public class RelationalOperator {
    public static final com.ryft.spark.connector.domain.RelationalOperator contains = contains$.MODULE$;
    public static final com.ryft.spark.connector.domain.RelationalOperator equals = equalTo$.MODULE$;
    public static final com.ryft.spark.connector.domain.RelationalOperator notContains = notContains$.MODULE$;
    public static final com.ryft.spark.connector.domain.RelationalOperator notEqualTo = notEqualTo$.MODULE$;
}

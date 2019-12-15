package com.examples.spark.conf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkConfig {

    public static JavaSparkContext getSparkContext(String appName) {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]");
        return new JavaSparkContext(conf);
    }

    public static SparkSession getSparkSession(String appName){
        return SparkSession.builder().appName(appName).master("local[*]").getOrCreate();
    }

    public static JavaStreamingContext getSparkStreamingContext(String appName,Long batchSize) {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]");
        return new JavaStreamingContext(conf, Durations.seconds(batchSize));
    }
}

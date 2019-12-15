package com.examples.spark.basics;

import com.examples.spark.conf.SparkConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Success;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

public class PairRDDExample {

    private static Tuple2<String,Integer> mapToResponse(String line) {
        String data = line.split(" ")[4];
        Tuple2<String, Integer> response;
        switch (data) {
            case "200":
                response = new Tuple2<>("Success", 1);
                break;
            case "400":
                response = new Tuple2<>("Bad Request", 1);
                break;
            case "304":
                response = new Tuple2<>("Not Modified", 1);
                break;
            default:
                response = new Tuple2<>("No Error code", 1);
        }
        return response;
    }
    public static void main(String[] args) {
        Logger.getLogger("apache.org").setLevel(Level.WARN);
        JavaSparkContext sc = SparkConfig.getSparkContext("Tuples Uses");

        JavaRDD<String> textFileData = sc.textFile("src/main/resources/web.log");
        JavaPairRDD<String, Integer> responsePair = textFileData.mapToPair(PairRDDExample::mapToResponse);
        responsePair.reduceByKey(Integer::sum).collectAsMap().forEach((key, value) -> System.out.println(key + " -> " + value));




    }
}

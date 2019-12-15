package com.examples.spark.basics;

import com.examples.spark.conf.SparkConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Tuples {

    public static void main(String[] args) {
        Logger.getLogger("apache.org").setLevel(Level.WARN);
        JavaSparkContext sc = SparkConfig.getSparkContext("Tuples Uses");

        List<Integer> list = Arrays.asList(1,23,32,21,56);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        JavaRDD<Tuple2<Integer, Double>> tupleData = listRDD.map(digit -> new Tuple2<>(digit, Math.sqrt(digit)));
        tupleData.foreach(data -> System.out.println("digit :" + data._1 + " square root is " + data._2));
        sc.close();

    }
}
package com.examples.spark.basics;

import com.examples.spark.conf.SparkConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LetsStart {

    public static void main(String[] args){
        Logger.getLogger("apache.org").setLevel(Level.WARN);
        JavaSparkContext sc = SparkConfig.getSparkContext("Spark Basics");

        List<Double> list = Arrays.asList(1.00,23.22,32.00,21.72,56.00);
        JavaRDD<Double> doubleRDD = sc.parallelize(list);
        doubleRDD.filter(data -> data > 30.00).collect().forEach(System.out::println);
        //if you want to add all the values just use reduce method.
        //doubleRDD.reduce((x, y) -> x + y); use lambda expression.
        Double doubleSum = doubleRDD.reduce(Double::sum);
        System.out.println(doubleSum);
        sc.close();


    }
}

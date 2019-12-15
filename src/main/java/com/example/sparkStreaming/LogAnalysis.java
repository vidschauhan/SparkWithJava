package com.example.sparkStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import static com.examples.spark.conf.SparkConfig.getSparkStreamingContext;

public class LogAnalysis {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
        JavaStreamingContext jsc = getSparkStreamingContext("Logging analysis",2L);


        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 8989); // logging server stream input;

        JavaPairDStream<String, Long> results = lines.mapToPair(data -> new Tuple2<>(data.split(",")[0], 1L));

        //Windowing
        results.reduceByKeyAndWindow(Long::sum,Durations.seconds(60)).print();

        jsc.start(); // start the stream.
        jsc.awaitTermination(); // run infinitely till interrupted.


    }
}

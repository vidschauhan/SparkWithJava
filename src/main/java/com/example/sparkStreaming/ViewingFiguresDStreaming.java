package com.example.sparkStreaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

import static com.examples.spark.conf.SparkConfig.getSparkStreamingContext;

public class ViewingFiguresDStreaming {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
        JavaStreamingContext jsc = getSparkStreamingContext("Logging analysis", 1L);

        //Refer https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html for the kafka configs
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark-group");
        kafkaParams.put("auto.offset.reset", "latest");

        Collection<String> topics = Collections.singletonList("viewrecords"); // Name of the topic in kafka. you can use simple list Arrays.asList() too.

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams));

        JavaDStream<String> result = stream.map(ConsumerRecord::value); // or stream.map(data -> data.key());

        //Perform some aggregation on the stream data..

        // last param is for Slide duration. which needs to be multiple of Windowing duration.
        // will dispose data every 30 second because slide interval is 30 sec.
        // but the batch is processed every second. Check batch size.
        JavaPairDStream<Long, String> output = result.mapToPair(data -> new Tuple2<>(data, 5L))
                .reduceByKeyAndWindow(Long::sum, Durations.minutes(60), Durations.seconds(30))
                .mapToPair(Tuple2::swap) // Swapping keys value positions
                .transformToPair(data -> data.sortByKey(false)); // Transforming to RDD to apply sort on

        output.print(50);

        jsc.start();
        jsc.awaitTermination();


    }
}

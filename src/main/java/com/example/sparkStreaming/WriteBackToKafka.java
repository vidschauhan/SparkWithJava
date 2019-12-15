package com.example.sparkStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static com.examples.spark.conf.SparkConfig.getSparkSession;

public class WriteBackToKafka {



        public static void main(String[] args) throws InterruptedException, StreamingQueryException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkSession session = getSparkSession("Structured Streaming");
        session.conf().set("spark.sql.shuffle.partitions",8); // performance tuning
        /*Dataset<Row> dataFrame = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "viewrecords")
                .load();

        dataFrame.createOrReplaceTempView("viewing_records");
*/

             session
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "viewrecords")
                    .load().createOrReplaceTempView("record_view");

                    session.sql("select window,CAST(value as STRING) as courses,count(5) as view_time from record_view  group by window('timestamp','2 minutes'),courses")
                            .withWatermark("window", "1 minutes")
                    .writeStream()
                    .format("kafka")
                            .outputMode(OutputMode.Append())
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("topic", "test")
                            .option("checkpointLocation", "/tmp")
//                            .trigger(Trigger.Continuous("1 second"))
                      // only change in query
                    .start().awaitTermination();


    }

}

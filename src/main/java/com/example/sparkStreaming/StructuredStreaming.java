package com.example.sparkStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static com.examples.spark.conf.SparkConfig.getSparkSession;

public class StructuredStreaming {

    public static void main(String[] args) throws InterruptedException, StreamingQueryException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkSession session = getSparkSession("Structured Streaming");
        session.conf().set("spark.sql.shuffle.partitions",8); // performance tuning
        Dataset<Row> dataFrame = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "viewrecords")
                .load();

        dataFrame.createOrReplaceTempView("viewing_records");
        Dataset<Row> results = session.sql("select window,cast(value as string) as courses,count(5) as view_time  from viewing_records group by window('timestamp','2 minutes'),courses");
        // Writing into console sink
        StreamingQuery query = results.withWatermark("window", "1 seconds").writeStream()
                .format("console")
                .outputMode(OutputMode.Complete())
                .option("truncate",false)
                .option("numRows",50).start();
        query.awaitTermination();


    }
    }

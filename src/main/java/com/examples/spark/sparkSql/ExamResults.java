package com.examples.spark.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import static com.examples.spark.conf.SparkConfig.getSparkSession;

public class ExamResults {

    public static void main(String[] args) {
        Logger.getLogger("apache.org").setLevel(Level.WARN);
        SparkSession sparkSession = getSparkSession("Spark Sql"); // static import

        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/students.csv");

        dataset = dataset.groupBy("subject").agg(max(col("score")).alias("max score"),
                                                        min(col("score")).alias("min score"));
        dataset.show();

        sparkSession.close();


    }
}

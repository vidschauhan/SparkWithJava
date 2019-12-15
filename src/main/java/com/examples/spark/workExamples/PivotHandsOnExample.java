package com.examples.spark.workExamples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import static com.examples.spark.conf.SparkConfig.getSparkSession;
import static org.apache.spark.sql.functions.*;

public class PivotHandsOnExample {

    public static void main(String[] args) {
        Logger.getLogger("apache.org").setLevel(Level.WARN);
        SparkSession sparkSession = getSparkSession("Spark Sql"); // static import

        //Just to optimize the runtime of program. so that many task are not sitting idol. helpful in multiple stages and shuffles.
        sparkSession.conf().set("spark.sql.shuffle.partitions",8);
        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/students.csv");

        Object[] years = new Object[] { "2005","2006","2007","2008","2009","2010","2011","2012","2013","2014"};
        List<Object> year_col = Arrays.asList(years);

        /*dataset = dataset.groupBy("subject").agg(max(col("score")).alias("max score"),
                min(col("score")).alias("min score"));*/

        dataset = dataset.groupBy("subject").pivot("year", year_col)
                        .agg(
                                round(avg(col("score")),2).alias("average"),
                                round(stddev(col("score")),2).alias("Std_dev")
                        );
        dataset.show(100);
        new Scanner(System.in).nextLine();
        sparkSession.close();

    }
}

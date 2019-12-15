package com.examples.spark.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

import static com.examples.spark.conf.SparkConfig.getSparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class PivotTables {


    public static void main(String[] args) {
        Logger.getLogger("apache.org").setLevel(Level.WARN);
        SparkSession sparkSession = getSparkSession("Spark Sql"); // static import

        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/biglog.txt");
        dataset = dataset.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) );


        Object[] months = new Object[] { "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
        List<Object> columns = Arrays.asList(months);

        dataset = dataset.groupBy("level").pivot("month", columns).count();

        dataset.show(100);

        sparkSession.close();


    }
}

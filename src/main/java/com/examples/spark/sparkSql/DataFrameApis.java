package com.examples.spark.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static com.examples.spark.conf.SparkConfig.getSparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class DataFrameApis {

    public static void main(String[] args) {
        Logger.getLogger("apache.org").setLevel(Level.WARN);
        SparkSession sparkSession = getSparkSession("Spark Sql"); // static import

        Dataset<Row> logData = sparkSession.read().option("header", true).csv("src/main/resources/biglog.txt");
      /*  logData.createOrReplaceTempView("log_table");
        Dataset<Row> formattedDate =
                sparkSession.sql("Select level, date_format(datetime,'MMMM') as month, count(1) as total " +
                        "from log_table" +
                        "group by level, month " +
                        "order by cast(first(date_format(datetime,'M')) as int),level");*/

        Dataset<Row> dataset = logData.select(col("level"), date_format(col("datetime"), "MMMM").alias("month"),
                                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));

                    dataset = dataset.groupBy(col("level"),col("month"),col("monthnum")).count();
                    dataset = dataset.orderBy(col("monthnum")
                            ,col("level")).drop(col("monthnum"));

                    dataset.show();





    }
}

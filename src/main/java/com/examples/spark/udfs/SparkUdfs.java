package com.examples.spark.udfs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static com.examples.spark.conf.SparkConfig.getSparkSession;
import static org.apache.spark.sql.functions.*;

public class SparkUdfs {

    public static void main(String[] args) {
        //User defined functions.

        Logger.getLogger("apache.org").setLevel(Level.WARN);
        SparkSession sparkSession = getSparkSession("Spark Sql"); // static import


        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/students.csv");


//        Register uds using udf.register. (function Name, function body, return type of the function).
        sparkSession.udf().register("hasPassed",grade -> grade.equals("A+"), DataTypes.BooleanType);

//        Complex method defination UDFs
        sparkSession.udf().register("hasPassed",(String grade,String subject) ->
                {
                    if (subject.equalsIgnoreCase("Biology")) {
                        return (grade.startsWith("A"));
                    } else {
                        return (grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C"));
                    }
                },DataTypes.BooleanType);

//            to add new columns on the fly just use dataset.withColumn()
//        dataset = dataset.withColumn("pass", lit("yes"));

        dataset = dataset.withColumn("pass", callUDF("hasPassed",col("grade"),col("subject")));
        dataset.show();

        sparkSession.close();

    }
}

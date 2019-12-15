package com.examples.spark.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import static com.examples.spark.conf.SparkConfig.getSparkSession;
import static org.apache.spark.sql.functions.col;

public class BasicsSparkSql {

    public static void main(String[] args) {
        Logger.getLogger("apache.org").setLevel(Level.WARN);
        SparkSession sparkSession = getSparkSession("Spark Sql"); // static import

        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/students.csv");
        //dataset.show(); //Datasets are immutable. always returns a new instance. Lazily evaluating

        Row firstRow = dataset.first(); // extract first row from table

        String subject = (String) firstRow.get(2); // index of the column
        String sub =  firstRow.getAs("subject"); // name of the column // does automatic datatype conversion

        System.out.println("getAs ->" + sub + " " + "get ->" + subject);

        //dataset.filter("subject = 'Math' AND year >=2008").show();

        //Another version
         /*dataset.filter((FilterFunction<Row>) row ->
              row.getAs("subject").equals("Math") &&
                      Integer.parseInt(row.getAs("year")) >= 2006).show();*/

         //Another version
        Column subjectCol = functions.col("subject");
        Column yearCol = dataset.col("year");

        dataset.filter(subjectCol.equalTo("Math").and(yearCol.geq(2007))).show();


        //Static Import from Class functions  in spark
        dataset.filter(col("subject").equalTo("Math").and(col("year").geq(2007))).show();

        dataset.createOrReplaceTempView("my_student");
        sparkSession.sql("select * from my_student where year = 2009 ").show();
        
        sparkSession.close();
    }
}

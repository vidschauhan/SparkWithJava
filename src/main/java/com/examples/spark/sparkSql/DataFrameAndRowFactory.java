package com.examples.spark.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static com.examples.spark.conf.SparkConfig.getSparkSession;

public class DataFrameAndRowFactory {
    public static void main(String[] args) {
        Logger.getLogger("apache.org").setLevel(Level.WARN);
        SparkSession sparkSession = getSparkSession("Spark Sql"); // static import

        List<Row> inMemory = new ArrayList<>();
        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

        StructField[] structFields = new StructField[]{
                new StructField("level", DataTypes.StringType,false, Metadata.empty()),
                new StructField("date", DataTypes.StringType,false, Metadata.empty())
        };

        StructType schema = new StructType(structFields);
        Dataset<Row> dataset = sparkSession.createDataFrame(inMemory,schema);
        dataset.show();

        dataset.createOrReplaceTempView("log_table");
        //Produces group of list result when group by
        Dataset<Row> result = sparkSession.sql("Select level, collect_list(date) " +
                                                        "from log_table group by level order by level");

        Dataset<Row> formattedDate = sparkSession.sql("Select level, date_format(date,'MMMM') as month " +
                "                                               from log_table");
        formattedDate.show();
    }
}

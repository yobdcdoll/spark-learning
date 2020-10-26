package com.yobdcdoll.hive;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class HiveTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("HiveTest")
//                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        /**
         * UDF
         */
        spark.udf().register("upper_word", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception {
                if (s != null) {
                    return s.toUpperCase();
                }
                return null;
            }
        }, DataTypes.StringType);

        spark.udf().register("movieUDAF", new MovieUDAF());

//        Dataset<Row> moviesDs = spark.read()
//                .format("csv")
//                .option("inferSchema", "true")
//                .option("header", "true")
//                .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS")
//                .load("input/movies.csv");
//
//        moviesDs.createOrReplaceTempView("movies");
//
//        spark.sql("create temporary function movieUDTF as 'com.yobdcdoll.hive.MovieUDTF'");
//
//        spark.sql("select movieUDTF(genres) genre from movies")
//                .show();

        spark.sql("insert overwrite table users select * from users");
//        spark.sql("show tables");

        spark.stop();
    }
}

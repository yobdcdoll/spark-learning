package com.yobdcdoll.metrics;

import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.metrics.source.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.util.List;

public class MetricsTest {
    public static void main(String[] args) throws IOException {

        SparkSession spark = SparkSession.builder()
                .appName("MetricsTest")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> moviesDs = spark.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS")
                .load("input/movies.csv");

        MySource myMapCount = new MySource("myMapGram");
        spark.sparkContext().env().metricsSystem().registerSource(myMapCount);

        Dataset<String> moviesDs2 = moviesDs.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row value) throws Exception {
                Seq<Source> myMapCount2 = SparkEnv.get().metricsSystem()
                        .getSourcesByName("myMapGram");
                List<Source> sources = JavaConverters.seqAsJavaList(myMapCount2);
                MySource src = (MySource) sources.get(0);
                src.inc(System.currentTimeMillis());
                return value.getString(1);
            }
        }, Encoders.STRING());

        moviesDs2.show(100);

        System.in.read();

        spark.stop();
    }
}

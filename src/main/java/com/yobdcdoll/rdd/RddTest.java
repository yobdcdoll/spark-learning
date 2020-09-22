package com.yobdcdoll.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import scala.Tuple3;

public class RddTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("RddTest");

        JavaSparkContext sc = new JavaSparkContext(conf);

        LongAccumulator printCount = sc.sc().longAccumulator("printCount");

        Broadcast<Integer> max = sc.broadcast(5);

        JavaRDD<String> moviesRDD = sc.textFile("/input/movies/movies.csv");
        moviesRDD
                .map(new Function<String, Tuple3<Long, String, String>>() {
                    @Override
                    public Tuple3<Long, String, String> call(String s) throws Exception {
                        String[] arr = s.split(",");
                        if (arr == null || arr.length != 3 || "movieId".equals(arr[0])) {
                            return Tuple3.apply(null, null, null);
                        }
                        Long movieId = Long.parseLong(arr[0]);
                        String title = arr[1];
                        String genres = arr[2];

                        return Tuple3.apply(movieId, title, genres);
                    }
                })
                .filter(new Function<Tuple3<Long, String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple3<Long, String, String> val) throws Exception {
                        return val._1() != null;
                    }
                })
                .mapToPair(new PairFunction<Tuple3<Long, String, String>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple3<Long, String, String> val) throws Exception {
                        return Tuple2.apply(val._3(), val._1());
                    }
                })
                .combineByKey(new Function<Long, Long>() {
                    @Override
                    public Long call(Long v1) throws Exception {
                        return v1;
                    }
                }, new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }, new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                })
                .coalesce(1)
                .foreach(new VoidFunction<Tuple2<String, Long>>() {
                    long count = 0;

                    @Override
                    public void call(Tuple2<String, Long> val) throws Exception {
                        count++;
                        if (count > max.getValue()) {
                            return;
                        }
                        printCount.add(1);
                        System.out.println(val);
                    }
                });

        System.out.println(printCount.value());

        sc.stop();
    }
}

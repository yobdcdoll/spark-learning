package com.yobdcdoll.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WordCountTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("WordCountTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> movies = sc.textFile("input/movies.csv");
        movies
                .flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
                    @Override
                    public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                        String[] split = s.split(",");
                        List<Tuple2<String, Integer>> result = new ArrayList<>();
                        if (split != null && split.length == 3) {
                            String[] genres = split[2].split("\\|");
                            for (int i = 0; genres != null && i < genres.length; i++) {
                                result.add(Tuple2.apply(genres[i], 1));
                            }
                        }
                        return result.iterator();
                    }
                })
                .filter(new Function<Tuple2<String, Integer>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Integer> val) throws Exception {
                        return !"genres".equals(val._1);
                    }
                })
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                })
                .map(new Function<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> v1) throws Exception {
                        return v1;
                    }
                })
                .sortBy(new Function<Tuple2<String, Integer>, Integer>() {
                    @Override
                    public Integer call(Tuple2<String, Integer> v1) throws Exception {
                        return v1._2;
                    }
                }, false, 1)
                .foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> v) throws Exception {
                        System.out.println(v);
                    }
                });

        sc.stop();
    }
}

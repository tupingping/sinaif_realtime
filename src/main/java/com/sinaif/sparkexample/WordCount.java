package com.sinaif.sparkexample;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount {
    public static void main(String[] args) {
        wordCount1();
    }

    public static void wordCount1(){
        SparkConf sparkConf = new SparkConf().setAppName("reducebykey").setMaster("local");
        SparkContext sc = new SparkContext(sparkConf);
        String filePath = "/Users/tupingping/datasource/spark_test/test.txt";
        JavaRDD<String> lines = sc.textFile(filePath, 1).toJavaRDD();

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] d = s.split(" ");
                return Arrays.asList(d).iterator();
            }
        });

        JavaPairRDD<String, Integer> wp = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> wpc = wp.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(wpc.collect());

        sc.stop();

    }

    public static void wordCount2(){


    }
}

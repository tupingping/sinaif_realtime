package com.sinaif.sparkexample;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;


public class TopNByGroup {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test topN").setMaster("local");
        SparkContext sparkContext = new SparkContext(conf);
        String filePath = "/Users/tupingping/datasource/spark_test/test.txt";
        JavaRDD<String> lines = sparkContext.textFile(filePath,1).toJavaRDD();

        JavaPairRDD<String, Integer> pairRdd = lines.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] tmp = s.split(" ");
                return new Tuple2<>(tmp[0], Integer.valueOf(tmp[1]));
            }
        });

        // 分组, 排序
        JavaPairRDD<String, Iterable<Integer>> pairRDD1 = pairRdd.groupByKey().sortByKey();

        // top n
        pairRDD1.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                String name = stringIterableTuple2._1();
                Iterator<Integer> it = stringIterableTuple2._2().iterator();

                Integer[] res = new Integer[3];

                while (it.hasNext()){
                    Integer s = it.next();

                    for(int i = 0; i < 3; i++){
                        if(res[i] == null){
                            res[i] = s;
                            break;
                        }else if(res[i]<s){
                            for (int j = 2; j > i; j--) {
                                res[j] = res[j - 1];
                            }
                            res[i] = s;
                            break;
                        }
                    }
                }

                System.out.println(name+":");
                for(Integer a : res){
                    System.out.println(a +"\t");
                }

            }
        });

        sparkContext.stop();

    }


}

package com.itcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WordCount1 {
    public static void main(final String[] args) {
        //创建sparkConf对象 设置appName和master地址
        SparkConf sparkConf = new SparkConf().setAppName("localJavaCount").setMaster("local[2]");
        //创建sparkcontext对象
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //读取数据文件
        JavaRDD<String> contextRDD = javaSparkContext.textFile("F:\\1.txt");
        //切分
        JavaRDD<String> wordRDD = contextRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) {
                String[] arr = s.split(" ");
                return Arrays.asList(arr).iterator();
            }
        });
        //叠加
        JavaPairRDD<String, Integer> wordOneRDD = wordRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        ////出现次数相加
        JavaPairRDD<String, Integer> resultRDD = wordOneRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        //排序
        JavaPairRDD<Integer, String> sortRDD = resultRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<Integer, String>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });
        JavaPairRDD<Integer, String> javaPairRDD = sortRDD.sortByKey(false);
        // 再对调位置
        JavaPairRDD<String, Integer> finalResultRDD = sortRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return new Tuple2<String, Integer>(tuple2._2, tuple2._1);
            }
        });
        // 7.收集结果数据
        List<Tuple2<String, Integer>> collectRDD = finalResultRDD.collect();
        for (Tuple2<String, Integer> tuple : collectRDD) {
            System.out.println("单词:" +tuple._1+"出现"+tuple._2);
        }
        javaSparkContext.stop();

    }
}

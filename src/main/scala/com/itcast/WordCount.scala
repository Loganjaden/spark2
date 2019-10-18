package com.itcast

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //创建SparkConf对象  设置appName地址  local[2] 表示本地2线程
    val conf: SparkConf = new SparkConf().setAppName("WordCount")
      //.setAppName("LocalWordCount").setMaster("local[2]")
    //1  创建SparkContext
    val context: SparkContext = new SparkContext(conf)
    //设置输出级别
    context.setLogLevel("WARN")
    //2 读取数据
    val dataRDD: RDD[String] = context.textFile(args(0))
      //.textFile("F:\\大数据就业班资料\\spark\\spark_teach_day04\\input\\1.txt")
    //3 切分文件每行,返回文件所有单词
    val wordsRDD: RDD[String] = dataRDD.flatMap(_.split(" "))
    //4 每个单词计数1
    val wordsAndOneRDD: RDD[(String, Int)] = wordsRDD.map((_, 1))
    //5 相同单词进行累加
    val resultRDD: RDD[(String, Int)] = wordsAndOneRDD.reduceByKey(_ + _)
    //6 按照单词出现次数降序排序
    val sortResultRDD: RDD[(String, Int)] = resultRDD.sortBy(_._2, false)
    //7 收集结果数据
    val tuples: Array[(String, Int)] = sortResultRDD.collect()
    //8 打印数据结果
//    for (tuple <- tuples) {
//      println("数据结果  -> " + tuple)
//    }
    sortResultRDD.saveAsTextFile(args(1))
    //9 关闭sparkContext
    context.stop()
  }
}

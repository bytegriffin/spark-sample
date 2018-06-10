package com.bytegriffin;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

@SuppressWarnings({ "unused", "resource" })
public class WordCount {

	private static void run_hdfs_file() {
		SparkConf conf = new SparkConf().setAppName("WordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("hdfs://centos1:9000/123.txt");
		JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey((i1, i2) -> i1 + i2);
		List<Tuple2<String, Integer>> output = counts.collect();
		output.stream().forEach(System.out::println);
	}
	
	private static void run_local_file() {
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("/opt/123.txt");
		JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey((i1, i2) -> i1 + i2);
		List<Tuple2<String, Integer>> output = counts.collect();
		output.stream().forEach(System.out::println);
	}

	public static void main(String[] args) {
		run_local_file();
	}

}

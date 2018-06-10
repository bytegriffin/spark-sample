package com.bytegriffin;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CollectionParallelize {
	
	private static void run_local_collection() {
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numbers = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> lines = sc.parallelize(numbers);
		int sum = lines.reduce((i1,i2) -> i1+i2);
		System.out.println(sum);
		sc.close();
	}

	public static void main(String[] args) {
		run_local_collection();
	}

}

package com.sriharilabs;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRddReadWholeTextFile {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Demo1").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaPairRDD<String, String> distData=sc.wholeTextFiles("data");
		distData.foreach(s->{
			System.out.println("\n "+s);
		});
		
		
//		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
//		JavaRDD<Integer> distData = sc.parallelize(data);
//		System.out.println(distData.count());
//		distData.saveAsTextFile("output");
//		distData.saveAsObjectFile("output1");
		
	}

}

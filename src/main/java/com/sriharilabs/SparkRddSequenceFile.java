package com.sriharilabs;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRddSequenceFile {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Demo1").setMaster("local");
		//JavaSparkContext sc = new JavaSparkContext(conf);
//		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
//		JavaRDD<Integer> distData = sc.parallelize(data);
//		System.out.println(distData.count());
//		distData.saveAsTextFile("output");
//		distData.saveAsObjectFile("output1");
		
		//sc.sequenceFile("data", keyClass, valueClass)
		
//		String st="[(‘key1’, 1.0), (‘key2’, 2.0), (‘key3’, 3.0)]";
//		System.out.println(Arrays.asList("srihari, 2.3","prasad,4"));
//		JavaRDD<String> distData =sc.parallelize(Arrays.asList("srihari, 2.3","prasad,4"));
//		
//		distData.foreach(d->{
//			System.out.println(d);
//		});
		
		JavaSparkContext ctx = new JavaSparkContext(conf);

	    //STEP1: READ
	    JavaPairRDD<Text, BytesWritable> rdd = ctx.sequenceFile("data", Text.class, BytesWritable.class);
	    
	    rdd.foreach(f->{
	    	System.out.println(f);
	    });
	}

}

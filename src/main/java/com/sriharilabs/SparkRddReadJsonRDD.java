package com.sriharilabs;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkRddReadJsonRDD {

	public static void main(String[] args) {

		//SparkConf conf = new SparkConf().setAppName("Demo1").setMaster("local");
		SparkSession spark = SparkSession
		        .builder()
		        .appName("Spark Example - Write Dataset to JSON File")
		        .master("local[2]")
		        .getOrCreate();
		
		String jsonPath = "data/sample.json";
        JavaRDD<Row> items = spark.read().json(jsonPath).toJavaRDD();
        items.foreach(item -> {
            System.out.println(item); 
        });
        
        //items.saveAsObjectFile("jsonOutput");
        items.saveAsTextFile("jsonoput");
        
	}

}

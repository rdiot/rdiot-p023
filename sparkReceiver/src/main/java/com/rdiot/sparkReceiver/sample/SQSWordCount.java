package com.rdiot.sparkReceiver.sample;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.amazonaws.regions.Regions;
import com.rdiot.sparkReceiver.receiver.SQSReceiver;

import scala.Tuple2;

public class SQSWordCount {
	
	private static Logger logger = Logger.getLogger(SQSWordCount.class);
	
    final static String appName = "sparkSQSReceiver";
    final static String master = "local[2]";
    final static String queueName = "jobQueue";
    
    final static Duration batchDuration = Durations.seconds(5); // Batch Duration 
    final static Duration windowDuration = Durations.seconds(5); // TBD
    final static Duration slideDuration = Durations.seconds(3); // TBD

	public static void main(String[] args) throws InterruptedException {
		
    	Logger.getLogger("org").setLevel(Level.OFF);
		
    	//Spark Config 
        SparkConf conf = new SparkConf().setMaster(master).setAppName(appName);
        conf.set("spark.testing.memory", "2147480000");  // if you face any memory issues

		try (JavaStreamingContext jssc = new JavaStreamingContext(conf, batchDuration)) {
			SQSReceiver javaSQSReceiver = new SQSReceiver(queueName) // 메시지큐 삭제 
					.with(Regions.AP_NORTHEAST_2);
			
	        System.out.println("# Spark Streaming Start");
	        
			JavaReceiverInputDStream<String> input = jssc.receiverStream(javaSQSReceiver);
			
			// SQS Messages
			/*
	        input.foreachRDD(rdd->{
	            rdd.foreach(w->{
	            	System.out.println(w);
	            });
	        });
	        */
			
	        // Word Count
	        JavaDStream<String> words = input.flatMap(x -> Arrays.asList(x.split(":")).iterator());
	        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
	        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);        
	        wordCounts.print();

			
			jssc.start();
			jssc.awaitTermination();
			
		} finally {
			logger.info("Exiting the Application");
		}
	}

}

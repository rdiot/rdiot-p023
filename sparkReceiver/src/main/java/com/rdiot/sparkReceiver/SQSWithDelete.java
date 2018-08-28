package com.rdiot.sparkReceiver;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.amazonaws.regions.Regions;
import com.rdiot.sparkReceiver.receiver.SQSReceiver;

import scala.Tuple2;

public class SQSWithDelete {
	
	private static Logger logger = Logger.getLogger(SQSWithoutDelete.class);
	
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
		try (JavaStreamingContext jssc = new JavaStreamingContext(conf, batchDuration)) {
			SQSReceiver javaSQSReceiver = new SQSReceiver(queueName) // 메시지큐 삭제 
					.with(Regions.AP_NORTHEAST_2);
			
	        System.out.println("# Spark Streaming Start");
	        
			JavaReceiverInputDStream<String> input = jssc.receiverStream(javaSQSReceiver);
			
			// SQS 메시지 확인 
			/*
	        input.foreachRDD(rdd->{
	            rdd.foreach(w->{
	            	System.out.println(w);
	            });
	        });
	        */
			
			JavaPairDStream<String, Integer> messages = input.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
			messages.print();

			
			jssc.start();
			jssc.awaitTermination();
			
		} finally {
			logger.info("Exiting the Application");
		}
	}

}

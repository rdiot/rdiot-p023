package com.rdiot.sparkReceiver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rdiot.sparkReceiver.receiver.SQSReceiver;

import scala.Tuple2;

public class SQSWithoutDelete {
	
	private static Logger logger = Logger.getLogger(SQSWithoutDelete.class);
	
    final static String appName = "sparkSQSReceiver";
    final static String master = "local[*]";
    final static String queueName = "jobQueue";
    
    final static Duration batchDuration = Durations.seconds(5); // Batch Duration 
    final static Duration windowDuration = Durations.seconds(5); // TBD
    final static Duration slideDuration = Durations.seconds(3); // TBD

	public static void main(String[] args) throws InterruptedException {
		
    	//Logger.getLogger("org").setLevel(Level.OFF);
		
    	//Spark Config 
        SparkConf conf = new SparkConf().setMaster(master).setAppName(appName);
		try (JavaStreamingContext jssc = new JavaStreamingContext(conf, batchDuration)) {
			SQSReceiver javaSQSReceiver = new SQSReceiver(queueName, false) // 메시지큐 삭제 후처리
					.with(Regions.AP_NORTHEAST_2);
			
	        System.out.println("# Spark Streaming Start");
	        
			JavaReceiverInputDStream<String> input = jssc.receiverStream(javaSQSReceiver);
			
			// SQS 메시지 확인 
			/* {"messageId":"8046663a-6c39-4e7c-85d9-19e1ca5a1422","receiptHandle":"AQEBWJgRParpcOX/USbqgJrt2ySrGmjfcKW9A6nGKrPK0UzEu2BOpD3U8qcypekzDdGBcXNS8rtMh2LYdys7RXGS6omo0faOLVuBbJff0nCJq9zTO7HUWYeTo52QuiTa3nn7OsuDA27CPLLtObo9Dnl8ETDPhWpLLZ/haAL6GHX5RFu0YbeQatcTNX8JetroYEeJn0zHAFsbFbFnjin1ISMmXbTOwFdbs9I35OTCwzIrqjXNObizpjxUWit3Chkn1+peevbPoCHzlpuqv22b8ZP9/eDS8ODF31bYnCW2FNUyWORtYQRAcPhQMwRsRatFeBg9HNHvDQEGOmU65rjWf4j1nf12wQ/iqGAhGtArPB4u8CRv+HyphmmBOdUcc1AxsH3acxVzh6McgOAwC6ryb6obzA==","body":"Car Car River","attributes":{},"messageAttributes":{},"md5OfBody":"b5c0bbc9aae566125acfb63a606656fe","md5OfMessageAttributes":null}
	        input.foreachRDD(rdd->{
	            rdd.foreach(w->{
	            	System.out.println(w);
	            });
	        });
	        */

			
			JavaPairDStream<String, Integer> messages = input
					.mapPartitionsToPair(words -> processWords(words,queueName))
					.reduceByKey((a, b) -> a + b);
			
			messages.print();

			
			jssc.start();
			jssc.awaitTermination();
			
		} finally {
			logger.info("Exiting the Application");
		}
	}
	
	private static Iterator<Tuple2<String, Integer>> processWords(Iterator<String> words,String queueName) {
		List<Tuple2<String, Integer>> tupleList = new ArrayList<>();
		ObjectMapper mapper = new ObjectMapper();
		
		AmazonSQS amazonSQS = AmazonSQSClientBuilder.standard().withRegion(Regions.AP_NORTHEAST_2).build();
		//AmazonSQS amazonSQS = AmazonSQSClientBuilder.standard().withRegion(Regions.AP_NORTHEAST_2).withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();
		String sqsURL = amazonSQS.getQueueUrl(queueName).getQueueUrl();
		while (words.hasNext()) {
			String word = words.next();
			try {
				Message message = mapper.readValue(word, Message.class);
				tupleList.add(new Tuple2<String, Integer>(message.getBody(), 1));
				//System.out.println(message.getBody());
				amazonSQS.deleteMessage(new DeleteMessageRequest(sqsURL, message.getReceiptHandle()));
			} catch (Exception e) {
				logger.error("Failed to process message", e);
			}
		}
		return tupleList.iterator();
	}

}

package com.rdiot.sparkReceiver.receiver;

import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SQSReceiver extends Receiver<String>{
	
	private static final long serialVersionUID = -2122296127642671620L;
	private String sqsQueueName; // SQS Queue Name
	private Regions sqsRegion = Regions.DEFAULT_REGION;
	private transient AWSCredentialsProvider sqsCredentials;
	private transient Logger logger; // log4j
	Long timeout = 0L; // timeout
	ObjectMapper mapper; // databind
	boolean sqsDeleteOnReceipt; // SQS 삭제 여부
	
	//생성자(default) : 큐명, 큐삭제
	public SQSReceiver(String sqsQueueName) {
		this(sqsQueueName,true);
	}
	
	//생성자 
	public SQSReceiver(String sqsQueueName, boolean sqsDeleteOnReceipt) {
		super(StorageLevel.MEMORY_AND_DISK_2()); // Spark Storage Memory 삭제
		this.sqsQueueName = sqsQueueName;
		this.mapper = new ObjectMapper();
		this.logger = Logger.getLogger(SQSReceiver.class);
		this.sqsDeleteOnReceipt = sqsDeleteOnReceipt;
	}
	
	@Override
	public void onStart() {
		// Data Receive 스레드 시작 
		System.out.println("# SQSReceiver # Start");
		new Thread(this::receive).start();    
	}
	 
	@Override
	public void onStop() {
		// isStopped()가 false면 자동멈춤으로 이외 처리시 코드  
	}
	 
	// Data Receive 
	private void receive() {
		try {
			//Amazon SQS Credentials 생성 
			AmazonSQSClientBuilder amazonSQSClientBuilder = AmazonSQSClientBuilder.standard();
			if (sqsCredentials != null) {
				amazonSQSClientBuilder.withCredentials(sqsCredentials);
			}
			
			// 임시 수동 테스트
			//BasicAWSCredentials awsCreds = new BasicAWSCredentials("", "");
			//amazonSQSClientBuilder.withCredentials(new AWSStaticCredentialsProvider(awsCreds));
			
			
			amazonSQSClientBuilder.withRegion(sqsRegion); // SQS Region
			final AmazonSQS amazonSQS = amazonSQSClientBuilder.build(); 
			final String sqsUrl = amazonSQS.getQueueUrl(sqsQueueName).getQueueUrl(); //SQS Queue Url
			System.out.println("# SQSReceiver # Receiving messages from Amazon SQS Queue.");
			System.out.println("# SQSReceiver # Amzon SQS Queue URL:"+ sqsUrl);
			
			//Receive SQS Message
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsUrl);
			
			//System.out.println(receiveMessageRequest.toString());

			try {
				while (!isStopped()) {
					List<Message> messages = amazonSQS.receiveMessage(receiveMessageRequest).getMessages();
					
					//SQS 메시지 확인 
					for (final Message message : messages) {
		                System.out.println("Message");
		                System.out.println("  MessageId:     "
		                        + message.getMessageId());
		                System.out.println("  ReceiptHandle: "
		                        + message.getReceiptHandle());
		                System.out.println("  MD5OfBody:     "
		                        + message.getMD5OfBody());
		                System.out.println("  Body:          "
		                        + message.getBody());
		                for (final Entry<String, String> entry : message.getAttributes()
		                        .entrySet()) {
		                    System.out.println("Attribute");
		                    System.out.println("  Name:  " + entry.getKey());
		                    System.out.println("  Value: " + entry.getValue());
		                }
		            }
					
					
					if (sqsDeleteOnReceipt) { // 큐삭제
						
						if(!messages.isEmpty())
						{
							String recieptHandle = messages.get(0).getReceiptHandle();
							messages.stream().forEach(m -> store(m.getBody()));
							amazonSQS.deleteMessage(new DeleteMessageRequest(sqsUrl, recieptHandle));
						}
						
					}
					else { //큐 미삭제
						messages.stream().forEach(this::storeMessage); // ReceiptHandle 그대로 넘겨 후처리 삭제 
					}
					if (timeout > 0L)
						Thread.sleep(timeout);
				}
				restart("Trying to connect again");
				
			} catch (IllegalArgumentException e) {
				restart("Could not connect", e);
			} catch (Throwable e) {
				restart("Error Recieving Data", e);
			}
			
		 
		} catch (Throwable e) { //호출자 미보고 
			restart("Error encountered while initializing", e);
		}
		 
	}

	
	// Store Meesage
	private void storeMessage(Message m) {
		try {
			store(mapper.writeValueAsString(m));
		} catch (JsonProcessingException e) {
			logger.error("Unable to write message to streaming context");
		}
	}
	
	// Timeout 
	public SQSReceiver withTimeout(Long timeoutInMillis) {
		this.timeout = timeoutInMillis;
		return this;
	}	
	
	// SQS Region 
	public SQSReceiver with(Regions sqsRegion) {
		this.sqsRegion = sqsRegion;
		return this;
	}
	
	// with Credentials key
	public SQSReceiver withCredentials(String accessKeyId, String secretAccessKey) {
		this.sqsCredentials = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretAccessKey));
		return this;
	}

	// with Credentials provider
	public SQSReceiver withCredentials(AWSCredentialsProvider awsCredentialsProvider) {
		this.sqsCredentials = awsCredentialsProvider;
		return this;
	}
}
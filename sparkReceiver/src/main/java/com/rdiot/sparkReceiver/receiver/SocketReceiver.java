package com.rdiot.sparkReceiver.receiver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
public class SocketReceiver extends Receiver<String>{
	
 private static final long serialVersionUID = -5800148755161532052L;
 String host = null; //호스트 초기화 
 int port = -1; //포트 초기화
 public SocketReceiver(String h, int p) {
  super(StorageLevel.MEMORY_AND_DISK_2()); // Spark Storage Memory 삭제   
  host = h; port = p; 
 }
 @Override
 public void onStart() {
  // Data Receive 스레드 시작 
  new Thread(this::receive).start();   
 }
 @Override
 public void onStop() {
  // isStopped()가 false면 자동멈춤으로 이외 처리시 코드
 }
 
 private void receive() {
     Socket socket = null;
     String userInput = null;
     try {
       // 서버 소켓 연결
       socket = new Socket(host, port);
       // 소켓 입력 데이터를 버퍼리더 적재 
       BufferedReader reader = new BufferedReader(
         new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
       // 연결이 중료될때까지, 버퍼 리딩 연결이 끊길때까지 
       while (!isStopped() && (userInput = reader.readLine()) != null) {
         System.out.println("Received data '" + userInput + "'"); //received data 출력
         store(userInput); //spark 메모리로 received data로 저장
       }
       reader.close(); // 버퍼리더 닫기 
       socket.close(); // 소켓종료
       // 활성화시 재연결
       restart("Trying to connect again");
     } catch(ConnectException ce) {
       // 연결불가 
       restart("Could not connect", ce);
     } catch(Throwable t) {
       // 데이터 수신 불가 
       restart("Error receiving data", t);
     }
 }  
}
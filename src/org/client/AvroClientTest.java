package org.client;


public class AvroClientTest {
	public static void main(String[] args) {
		MyRpcClientFacade client = new MyRpcClientFacade();
		client.init("192.168.137.2", 44444);
		String sampleData = "Hello Flume!";
		for (int i = 0; i < 10; i++) {
			client.sendDataToFlume(sampleData);
		}

		client.cleanUp();
	}
}



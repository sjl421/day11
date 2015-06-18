package org.client;

import java.nio.charset.Charset;

import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

class MyRpcClientFacade {
	private RpcClient client;
	private String hostname;
	private int port;
	public void init(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
		this.client = RpcClientFactory.getDefaultInstance(hostname, port);
	}

	public void sendDataToFlume(String data) {
		Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
		try {
			client.append(event);
		} catch (Exception e) {
			client.close();
			client = null;
			e.printStackTrace();
		}
	}

	public void cleanUp() {
		client.close();
	}

}

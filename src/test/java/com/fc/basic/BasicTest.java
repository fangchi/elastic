package com.fc.basic;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

public class BasicTest {

	@Test
	public void testBasic() throws UnknownHostException{
		@SuppressWarnings("resource")
		Settings settings = Settings.builder()
        .put("cluster.name", "elasticsearch").build();
		TransportClient client = new PreBuiltTransportClient(settings)
		        .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
		client.close();
	}
}

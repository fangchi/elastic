package com.fc.basic;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

public class BasicTest extends BaseTest{

	@Test
	public void testBasic() throws UnknownHostException{
		@SuppressWarnings("resource")
		Settings settings = Settings.builder()
        .put("cluster.name", "elasticsearch").build();
		TransportClient client = new PreBuiltTransportClient(settings)
		        .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
		client.close();
		
		//1.基本概念
		//1.近实时  near-realtime  从落地数据到能够被检索有1秒钟的延时
		//2.实体
		//2.1  cluster 
		//A cluster is a collection of one or more nodes (servers) that together holds your entire data and provides federated indexing and search capabilities across all nodes
	}
}

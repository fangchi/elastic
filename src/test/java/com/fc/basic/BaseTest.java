package com.fc.basic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import junit.framework.TestCase;

public class BaseTest extends TestCase{

	TransportClient client;
	
	@Override
	protected void setUp() throws Exception {
		@SuppressWarnings("resource")
		Settings settings = Settings.builder()
        .put("cluster.name", "elasticsearch").build();
		client = new PreBuiltTransportClient(settings)
		        .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
	}
	
	@Override
	protected void tearDown() throws Exception {
		client.close();
	}
	
	protected TransportClient getClient(){
		return client;
	}
	
	/**
	 * 以行为单位读取文件，常用于读面向行的格式化文件
	 */
	public static List<String> readFileByLines(File file) {
		List<String> arrayList = new ArrayList<String>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int line = 1;
			// 一次读入一行，直到读入null为文件结束
			while ((tempString = reader.readLine()) != null) {
				if(StringUtils.isNoneEmpty(tempString)){
					arrayList.add(tempString);
				}
				// 显示行号
				line++;
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
		return arrayList;
	}
	
	
	/**
	 * 以行为单位读取文件，常用于读面向行的格式化文件
	 */
	public static String readFileByString(File file) {
		StringBuffer sBuffer = new StringBuffer();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			// 一次读入一行，直到读入null为文件结束
			while ((tempString = reader.readLine()) != null) {
				if(StringUtils.isNoneEmpty(tempString)){
					sBuffer.append("\n");
					sBuffer.append(tempString);
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
		return sBuffer.toString();
	}
}

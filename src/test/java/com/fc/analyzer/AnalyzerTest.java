package com.fc.analyzer;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.junit.Test;
import org.wltea.analyzer.cfg.Configuration;
import org.wltea.analyzer.lucene.IKAnalyzer;

import junit.framework.TestCase;

public class AnalyzerTest extends TestCase{

	@Test
	public void testTokenStream() throws IOException {
	    Analyzer analyzer = new WhitespaceAnalyzer();
	    String inputText = "This is a test text for token token ";
	    TokenStream tokenStream = analyzer.tokenStream("text", new StringReader(inputText));
	    //保存token字符串
	    /**
	     * {interface org.apache.lucene.analysis.tokenattributes.TypeAttribute=, 
    		interface org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute=, 
    		interface org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute=, 
    		interface org.apache.lucene.analysis.tokenattributes.OffsetAttribute=, 
    		interface org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute=, 
    		interface org.apache.lucene.analysis.tokenattributes.CharTermAttribute=, 
    		interface org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute=}
	     */
	    CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);//表示token的字符串信息。比如"I'm"
	    TypeAttribute typeAttribute = tokenStream.addAttribute(TypeAttribute.class);//TypeAttribute 表示token词典类别信息，默认为“Word”，比如I'm就属于<APOSTROPHE>，有撇号的类型
	    OffsetAttribute offsetAttribute = tokenStream.addAttribute(OffsetAttribute.class);//表示token的首字母和尾字母在原文本中的位置
	    PositionIncrementAttribute positionIncrementAttribute = tokenStream.addAttribute(PositionIncrementAttribute.class);//表示tokenStream中的当前token与前一个token在实际的原文本中相隔的词语数量，用于短语查询。比如： 在tokenStream中[2:a]的前一个token是[1:I’m ]，它们在原文本中相隔的词语数是1，则token="a"的PositionIncrementAttribute值为1
	    PayloadAttribute payloadAttribute = tokenStream.addAttribute(PayloadAttribute.class);
	    PositionLengthAttribute positionLengthAttribute = tokenStream.addAttribute(PositionLengthAttribute.class);
	    //在调用incrementToken()开始消费token之前需要重置stream到一个干净的状态
	    tokenStream.reset();
	    while (tokenStream.incrementToken()) {
	        //打印分词结果
	        System.out.println("[" + charTermAttribute + "]"
	        			+"type:["+typeAttribute.type()+"]"
	        			+"Offset:["+offsetAttribute.startOffset()+","+offsetAttribute.endOffset()+"]"
	        			+"payload:["+payloadAttribute.getPayload()+"]"
	        			+"position:["+positionIncrementAttribute.getPositionIncrement()+"]"
	        			+"positionLength:["+positionLengthAttribute.getPositionLength()+"]"
	        );
	    }
	    analyzer.close();
	}
	
	
	@Test
	public void testTokenStream2UseSmart() throws IOException {
		org.elasticsearch.common.settings.Settings.Builder builder = Settings.builder();
		builder.put("path.home","E:\\Elastic\\Elasticsearch\\6.5.4\\plugins\\ik\\"); //需要将config文件拷贝至target才能运行
		builder.put("use_smart", "true");
		Settings settings = builder.build();
		Environment environment = new Environment(settings, PathUtils.get(System.getProperty("java.io.tmpdir")));
		Configuration configuration = new Configuration(environment, settings);
	    Analyzer analyzer = new IKAnalyzer(configuration);
	    String inputText = "NBA2联盟将花费50000000元和二十五千克黄金收购百度，同时任命易朝华为CEO";
	    TokenStream tokenStream = analyzer.tokenStream("text", new StringReader(inputText));
	    //保存token字符串
	    CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
	    TypeAttribute typeAttribute = tokenStream.addAttribute(TypeAttribute.class);//TypeAttribute 表示token词典类别信息，默认为“Word”，比如I'm就属于<APOSTROPHE>，有撇号的类型
	    OffsetAttribute offsetAttribute = tokenStream.addAttribute(OffsetAttribute.class);//表示token的首字母和尾字母在原文本中的位置
	    PositionIncrementAttribute positionIncrementAttribute = tokenStream.addAttribute(PositionIncrementAttribute.class);//表示tokenStream中的当前token与前一个token在实际的原文本中相隔的词语数量，用于短语查询。比如： 在tokenStream中[2:a]的前一个token是[1:I’m ]，它们在原文本中相隔的词语数是1，则token="a"的PositionIncrementAttribute值为1
	    PayloadAttribute payloadAttribute = tokenStream.addAttribute(PayloadAttribute.class);
	    //在调用incrementToken()开始消费token之前需要重置stream到一个干净的状态
	    tokenStream.reset();
	    while (tokenStream.incrementToken()) {
	    	 //打印分词结果
	        System.out.println(
	        			"token[" + charTermAttribute + "]"
	        			+"type:["+typeAttribute.type()+"]"
	        			+"Offset:["+offsetAttribute.startOffset()+","+offsetAttribute.endOffset()+"]"
	        			+"payload:["+payloadAttribute.getPayload()+"]"
	        			+"position:["+positionIncrementAttribute.getPositionIncrement()+"]"
	        );
	    }
	    analyzer.close();
	}
	
	@Test
	public void testTokenStream2NotUseSmart() throws IOException {
		org.elasticsearch.common.settings.Settings.Builder builder = Settings.builder();
		builder.put("path.home","E:\\Elastic\\Elasticsearch\\6.5.4\\plugins\\ik\\"); //需要将config文件拷贝至target才能运行
		builder.put("use_smart", "false");
		Settings settings = builder.build();
		Environment environment = new Environment(settings, PathUtils.get(System.getProperty("java.io.tmpdir")));
		Configuration configuration = new Configuration(environment, settings);
	    Analyzer analyzer = new IKAnalyzer(configuration);
	    String inputText = "NBA2联盟将花费50000000元和二十五千克黄金收购百度，同时任命易朝华为CEO";
	    TokenStream tokenStream = analyzer.tokenStream("text", new StringReader(inputText));
	    //保存token字符串
	    CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
	    TypeAttribute typeAttribute = tokenStream.addAttribute(TypeAttribute.class);//TypeAttribute 表示token词典类别信息，默认为“Word”，比如I'm就属于<APOSTROPHE>，有撇号的类型
	    OffsetAttribute offsetAttribute = tokenStream.addAttribute(OffsetAttribute.class);//表示token的首字母和尾字母在原文本中的位置
	    PositionIncrementAttribute positionIncrementAttribute = tokenStream.addAttribute(PositionIncrementAttribute.class);//表示tokenStream中的当前token与前一个token在实际的原文本中相隔的词语数量，用于短语查询。比如： 在tokenStream中[2:a]的前一个token是[1:I’m ]，它们在原文本中相隔的词语数是1，则token="a"的PositionIncrementAttribute值为1
	    PayloadAttribute payloadAttribute = tokenStream.addAttribute(PayloadAttribute.class);
	    //在调用incrementToken()开始消费token之前需要重置stream到一个干净的状态
	    tokenStream.reset();
	    while (tokenStream.incrementToken()) {
	    	 //打印分词结果
	        System.out.println(
	        			"token[" + charTermAttribute + "]"
	        			+"type:["+typeAttribute.type()+"]"
	        			+"Offset:["+offsetAttribute.startOffset()+","+offsetAttribute.endOffset()+"]"
	        			+"payload:["+payloadAttribute.getPayload()+"]"
	        			+"position:["+positionIncrementAttribute.getPositionIncrement()+"]"
	        );
	    }
	    analyzer.close();
	}
	
	
	@Test
	public void testTokenStream2UseSmartCross() throws IOException {
		org.elasticsearch.common.settings.Settings.Builder builder = Settings.builder();
		builder.put("path.home","E:\\Elastic\\Elasticsearch\\6.5.4\\plugins\\ik\\"); //需要将config文件拷贝至target才能运行
		builder.put("use_smart", "true");
		Settings settings = builder.build();
		Environment environment = new Environment(settings, PathUtils.get(System.getProperty("java.io.tmpdir")));
		Configuration configuration = new Configuration(environment, settings);
	    Analyzer analyzer = new IKAnalyzer(configuration);
	    String inputText = "光合作用力";// vs  光合作用力量
	    TokenStream tokenStream = analyzer.tokenStream("text", new StringReader(inputText));
	    //保存token字符串
	    CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
	    TypeAttribute typeAttribute = tokenStream.addAttribute(TypeAttribute.class);//TypeAttribute 表示token词典类别信息，默认为“Word”，比如I'm就属于<APOSTROPHE>，有撇号的类型
	    OffsetAttribute offsetAttribute = tokenStream.addAttribute(OffsetAttribute.class);//表示token的首字母和尾字母在原文本中的位置
	    PositionIncrementAttribute positionIncrementAttribute = tokenStream.addAttribute(PositionIncrementAttribute.class);//表示tokenStream中的当前token与前一个token在实际的原文本中相隔的词语数量，用于短语查询。比如： 在tokenStream中[2:a]的前一个token是[1:I’m ]，它们在原文本中相隔的词语数是1，则token="a"的PositionIncrementAttribute值为1
	    PayloadAttribute payloadAttribute = tokenStream.addAttribute(PayloadAttribute.class);
	    //在调用incrementToken()开始消费token之前需要重置stream到一个干净的状态
	    tokenStream.reset();
	    while (tokenStream.incrementToken()) {
	    	 //打印分词结果
	        System.out.println(
	        			"token[" + charTermAttribute + "]"
	        			+"type:["+typeAttribute.type()+"]"
	        			+"Offset:["+offsetAttribute.startOffset()+","+offsetAttribute.endOffset()+"]"
	        			+"payload:["+payloadAttribute.getPayload()+"]"
	        			+"position:["+positionIncrementAttribute.getPositionIncrement()+"]"
	        );
	    }
	    analyzer.close();
	}
	
}

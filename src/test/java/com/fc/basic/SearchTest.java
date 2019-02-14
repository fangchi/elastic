package com.fc.basic;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import com.tcrm.support.json.JsonUtils;


public class SearchTest extends BaseTest {

	@Test
	public void testInitDataIndex() throws IOException {
		URL url = SearchTest.class.getClassLoader().getResource("data.txt");
		File file = new File(url.getFile());
		List<String> contents = readFileByLines(file);
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		int index = 0;
		for (Iterator<String> iterator = contents.iterator(); iterator.hasNext();) {
			String content = (String) iterator.next();
			index++;
			bulkRequest.add(client.prepareIndex("searchdata", "article")
			        .setSource(XContentFactory.jsonBuilder()
			                    .startObject()
			                        .field("row", index)
			                        .field("added_time", new Date())
			                        .field("age", (int)(1+Math.random() * 30 ))
			                        .field("content",content)
			                    .endObject()
			                  )
			        );
		}
		BulkResponse bulkResponse = bulkRequest.get();
		if (bulkResponse.hasFailures()) {
		    System.out.println("sss");
		}
	}
	
	@Test
	public void testSearch() throws IOException {
		SearchResponse response = client.prepareSearch("twitter", "searchdata")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        //.setQuery(QueryBuilders.termQuery("content", "郭襄"))                 // Query
		        //.setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
		        .setQuery(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
		        .setFrom(0)
		        .setSize(5).setExplain(true)
		        .addSort("row", SortOrder.ASC)
		        .get();
		
		long total = response.getHits().getTotalHits();
		System.out.println(total);
		for (int i = 0; i < response.getHits().getHits().length; i++) {
			SearchHit searchHit = response.getHits().getHits()[i];
			System.out.println(searchHit.getVersion()+":"+searchHit.getSourceAsString());
		}
//		List<Aggregation> aggregations =response.getAggregations().asList();
//		for (Iterator<Aggregation> iterator = aggregations.iterator(); iterator.hasNext();) {
//			Aggregation aggregation = (Aggregation) iterator.next();
//			System.out.println(aggregation.toString());
//		}
	}
	
	@Test
	public void testSearch2() throws IOException {
		SearchRequestBuilder srb1 = client
			    .prepareSearch("twitter").setQuery(QueryBuilders.rangeQuery("age").from(12).to(18)).setSize(1);
		SearchRequestBuilder srb2 = client
		    .prepareSearch("searchdata").setQuery(QueryBuilders.matchQuery("name", "kimchy")).setSize(1);
		MultiSearchResponse sr = client.prepareMultiSearch()
		        .add(srb1)
		        .add(srb2)
		        .get();

		// You will get all individual responses from MultiSearchResponse#getResponses()
		for (MultiSearchResponse.Item item : sr.getResponses()) {
		    SearchResponse response = item.getResponse();
		    System.out.println(response.getHits().getTotalHits());
		}
		
	}
	
	
	@Test
	public void testSearch3() throws IOException {
		Map<String, Object> initParam = new HashMap<>();
		initParam.put("_agg", new HashMap<>());
		SearchResponse sr = client.prepareSearch("searchdata")
		    .setQuery(QueryBuilders.rangeQuery("age").from(1).to(25))
		    .addAggregation(
		            AggregationBuilders.terms("agg1").field("age").size(10000)
		            	.subAggregation(AggregationBuilders.terms("agg3").field("row").size(2))
		    )
		    .setSize(10000)
		    .setTimeout(TimeValue.MINUS_ONE)
		    .addAggregation(
		            AggregationBuilders.histogram("agg2")
		                    .field("age").interval(2) //直方图
		    )
		    .addAggregation(
		            AggregationBuilders.max("agg4")
		                    .field("age")
		    )
		    .addAggregation(
		            AggregationBuilders.sum("agg5")
		                    .field("age") 
		    )
		    .addAggregation(
		            AggregationBuilders.sum("agg5")
		                    .field("age") 
		    )
//		    .addAggregation(
//		            AggregationBuilders.scriptedMetric("agg")
//		            	   .initScript(new Script(ScriptType.INLINE,Script.DEFAULT_SCRIPT_LANG,  "_agg['heights'] = []", initParam))
//		                   .mapScript(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,"if(doc[\"age\"].value >15) {_agg.heights.add(1)}else{_agg.heights.add(0)}",  Collections.emptyMap()))
//		                   .combineScript(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,"heights_sum = 0; for (t in _agg.heights) { heights_sum += t }; return heights_sum", Collections.emptyMap()))
//		                   .reduceScript(new Script(ScriptType.INLINE,Script.DEFAULT_SCRIPT_LANG,"heights_sum = 0; for (a in _aggs) { heights_sum += a }; return heights_sum",  Collections.emptyMap()))
//		    )
		    .setTerminateAfter(10000) 
		    .get();

		// Get your facet results
		Terms agg1 = sr.getAggregations().get("agg1");
		Histogram agg2 = sr.getAggregations().get("agg2");
		 for (Iterator<? extends Bucket> iterator = agg1.getBuckets().iterator(); iterator.hasNext();) {
			Bucket bucket = (Bucket) iterator.next();
			System.out.println(""); 
			System.out.println(bucket.getKey()); 
			Terms aggregation  = bucket.getAggregations().get("agg3");
			for (Iterator<? extends Bucket> iterator2 = aggregation.getBuckets().iterator(); iterator2.hasNext();) {
				Bucket bucket2 = (Bucket) iterator2.next();
				System.out.print("{"+bucket2.getKey()+":"+bucket2.getDocCount()+"}"); 
			}
		}
		System.out.println(""); 
		System.out.println(agg2);
		Aggregation agg4 = sr.getAggregations().get("agg4");
		System.out.println(agg4);
		Aggregation agg5 = sr.getAggregations().get("agg5");
		System.out.println(agg5);
//		Aggregation agg6 = sr.getAggregations().get("agg");
//		System.out.println(agg6);
	}
	
	@Test
	public void testSearch4() throws IOException {
		URL url = SearchTest.class.getClassLoader().getResource("config/scripts/template_gender.mustache");
		
		Map<String, Object> param = new HashMap<>();
		Map<String, String> param2 = new HashMap<>();
		param2.put("lang", "mustache");
		param2.put("source", readFileByString(new File(url.getFile())));
		param.put("script", param2);
		
		String scriptObj =JsonUtils.toJson(param);
		
		AcknowledgedResponse acknowledgedResponse = client.admin().cluster().preparePutStoredScript()
			.setId("template_gender")
			.setContent(new BytesArray(scriptObj), XContentType.JSON).get();
		Boolean ac = acknowledgedResponse.isAcknowledged();
		System.out.println(ac);
		
		Map<String, Object> template_params = new HashMap<>();
		template_params.put("age", 15);
		SearchResponse response = new SearchTemplateRequestBuilder(client)
			    .setScript("template_gender")                 
			    .setScriptType(ScriptType.STORED) 
			    .setScriptParams(template_params)             
			    .setRequest(new SearchRequest())              
			    .get()                                        
			    .getResponse(); 
		long total = response.getHits().getTotalHits();
		System.out.println(total);
		for (int i = 0; i < response.getHits().getHits().length; i++) {
			SearchHit searchHit = response.getHits().getHits()[i];
			System.out.println(searchHit.getVersion()+":"+searchHit.getSourceAsString());
		}
		
		
	}
	
	
	
	@Test
	public void testSearch5() throws IOException {
		URL url = SearchTest.class.getClassLoader().getResource("config/scripts/template_gender.mustache");
		
		Map<String, Object> param = new HashMap<>();
		Map<String, String> param2 = new HashMap<>();
		param2.put("lang", "mustache");
		param2.put("source", readFileByString(new File(url.getFile())));
		param.put("script", param2);
		
		String scriptObj =JsonUtils.toJson(param);
		
		AcknowledgedResponse acknowledgedResponse = client.admin().cluster().preparePutStoredScript()
			.setId("template_gender")
			.setContent(new BytesArray(scriptObj), XContentType.JSON).get();
		Boolean ac = acknowledgedResponse.isAcknowledged();
		System.out.println(ac);
		
		Map<String, Object> template_params = new HashMap<>();
		template_params.put("age", 15);
		SearchResponse response = new SearchTemplateRequestBuilder(client)
			    .setScript("template_gender")                 
			    .setScriptType(ScriptType.STORED) 
			    .setScriptParams(template_params)             
			    .setRequest(new SearchRequest())              
			    .get()                                        
			    .getResponse(); 
		long total = response.getHits().getTotalHits();
		System.out.println(total);
		for (int i = 0; i < response.getHits().getHits().length; i++) {
			SearchHit searchHit = response.getHits().getHits()[i];
			System.out.println(searchHit.getVersion()+":"+searchHit.getSourceAsString());
		}
		
		
	}
}

package com.fc.basic;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.junit.Test;

public class IndexTest extends BaseTest{

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
					.setRouting("xxxx") //指定存在哪个分片中 通过使用文档的 id 值的哈希值来控制 这时所有文档保存在一个分片中
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
	public void testCreateIndex() throws IOException{
		XContentBuilder builder = XContentFactory.jsonBuilder()
			    .startObject()
			        .field("user", "kimchy")
			        .field("postDate", new Date())
			        .field("message", "trying out Elasticsearch")
			    .endObject();
		
		IndexResponse response = client.prepareIndex("twitter", "_doc", "test_id")
		        .setSource(builder)
		        .get();
	}
	
	
	@Test
	public void testGetIndex() throws IOException{
		GetResponse response = client.prepareGet("twitter", "_doc", "test_id").get();
		System.out.println(response.getId());
		System.out.println(response.getType());
		System.out.println(response.getVersion());
		System.out.println(response.getIndex());
		System.out.println(response.getSourceAsString());
	}
	
	@Test
	public void testDeleteIndex() throws IOException{
		DeleteResponse  response = client.prepareDelete("twitter", "_doc", "test_id").get();
		System.out.println(response.getId());
		System.out.println(response.getType());
		System.out.println(response.getVersion());
		System.out.println(response.getSeqNo());
	}
	
	@Test
	public void testDeleteByQueryIndex() throws IOException{
		//localhost:9200/searchdata/article/_delete_by_query?conflicts=proceed
		/**
		 * {
			  "query": {
			    "match_all": {}
			  }
			}
		 */
	}
	
	
	
	@Test
	public void testGetProperties() throws IOException{
		/**
{
    "mappings": {
        "_doc": {
            "properties": {
                "id": { "type": "text" },
                "title":  { "type": "text"},
                "abstract": { "type": "text"},
                "author": {
                    "properties": {
                        "id": { "type": "text" },
                        "name": { "type": "text" }
                    }
                }
            }
        }
    }
}
		 */
	}
	
	@Test
	public void testRefresh() throws IOException{
		/**
		 * localhost:9200/searchdata/_refresh
		 * 这些将创建一个文档并立即刷新索引，使其可见：
		 * PUT /test/test/1?refresh
		 * {"test": "test"}
		 * PUT /test/test/2?refresh=true
		 * {"test": "test"}
		 */
	}

	@Test
	public void testUpdateIndex() throws IOException, InterruptedException, ExecutionException{
		XContentBuilder builder = XContentFactory.jsonBuilder()
			    .startObject()
			        .field("user", "kimchy")
			        .field("postDate", new Date())
			        .field("gender", "male")
			        .field("message", "trying out Elasticsearch")
			    .endObject();
		
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index("twitter");
		updateRequest.type("_doc");
		updateRequest.id("test_id");
		updateRequest.doc(builder);
		client.update(updateRequest).get();
	}
	
	@Test
	public void testbulkRequest() throws IOException, InterruptedException, ExecutionException{
		BulkRequestBuilder bulkRequest = client.prepareBulk();

		// either use client#prepare, or use Requests# to directly build index/delete requests
		bulkRequest.add(client.prepareIndex("twitter", "_doc", "1")
		        .setSource(XContentFactory.jsonBuilder()
		                    .startObject()
		                        .field("user", "kimchy")
		                        .field("postDate", new Date())
		                        .field("message", "trying out Elasticsearch")
		                    .endObject()
		                  )
		        );

		bulkRequest.add(client.prepareIndex("twitter", "_doc", "2")
		        .setSource(XContentFactory.jsonBuilder()
		                    .startObject()
		                        .field("user", "kimchy")
		                        .field("postDate", new Date())
		                        .field("message", "another post")
		                    .endObject()
		                  )
		        );

		BulkResponse bulkResponse = bulkRequest.get();
		if (bulkResponse.hasFailures()) {
		    // process failures by iterating through each bulk response item
		}
	}
	
	//重建索引
	@Test
	public void testUpdateByQuery() throws IOException, InterruptedException, ExecutionException{
		UpdateByQueryRequestBuilder updateByQuery = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
		updateByQuery.source("twitter").abortOnVersionConflict(false);
		BulkByScrollResponse response = updateByQuery.get();
		System.out.println(response.getTotal());
	}
	
	
	@Test
	public void testUpdateByQuery3() throws IOException, InterruptedException, ExecutionException{
		UpdateByQueryRequestBuilder updateByQuery = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
		updateByQuery.source("twitter")
		    .filter(QueryBuilders.termQuery("gender", "male")) //条件
		    .size(1000) //条数
		    .script(new Script(ScriptType.INLINE, "ctx._source.user = 'fc'", "1", Collections.emptyMap()));
		BulkByScrollResponse response = updateByQuery.get();
	}
	
	@Test
	public void testUpdateByQuery2() throws IOException, InterruptedException, ExecutionException{
		ListTasksResponse tasksList = client.admin().cluster().prepareListTasks()
			    .setActions(UpdateByQueryAction.NAME).setDetailed(true).get();
			for (TaskInfo info: tasksList.getTasks()) {
			    TaskId taskId = info.getTaskId();
			    BulkByScrollTask.Status status = (BulkByScrollTask.Status) info.getStatus();
			    // do stuff
			    System.out.println("taskId:"+taskId.getId()+",status:"+status.toString());
			}
	}
	
	//Copy 索引  可以按照条件
	@Test
	public void testReindex() throws IOException, InterruptedException, ExecutionException{
		BulkByScrollResponse response = ReindexAction.INSTANCE.newRequestBuilder(client)
			    .source("twitter")
				.destination("twitter_dest")
				.refresh(true)
				.abortOnVersionConflict(false)
			    .filter(QueryBuilders.matchQuery("gender", "male"))
			    .get();
		System.out.println(response.getSearchFailures());
//		{
//			  "took" : 147,
//			  "timed_out": false,
//			  "created": 120,
//			  "updated": 0,
//			  "deleted": 0,
//			  "batches": 1,
//			  "version_conflicts": 0,
//			  "noops": 0,
//			  "retries": {
//			    "bulk": 0,
//			    "search": 0
//			  },
//			  "throttled_millis": 0,
//			  "requests_per_second": -1.0,
//			  "throttled_until_millis": 0,
//			  "total": 120,
//			  "failures" : [ ]
//			}
	}
}

package com.fc.basic;

import java.io.IOException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import com.tcrm.support.json.JsonUtils;

public class AggregationTest extends BaseTest{

	@Test
	public void testAggregation() throws IOException {
		SearchResponse response = client.prepareSearch("twitter", "searchdata")
		        .setQuery(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
		        .addAggregation(
		        	AggregationBuilders.terms("age_count").field("age").subAggregation(
		        	AggregationBuilders.terms("added_time_count").field("added_time"))
		        )
		        .setSize(10000)
		        .addSort("age", SortOrder.DESC)
		        .execute().actionGet();
		 Terms aggregation = response.getAggregations().get("age_count");
		 for (Terms.Bucket bucket : aggregation.getBuckets()) {
            System.out.println("age=" + bucket.getKey()+ ";age数量=" + bucket.getDocCount());
            Terms terms2 = bucket.getAggregations().get("added_time_count");
            for (Terms.Bucket bucket2 : terms2.getBuckets()) {
                System.out.println("添加时间=" + bucket2.getKey() + ";添加时间数量=" + bucket2.getDocCount());
            }
         }
	}
	
	
	@Test
	public void testAggregation2() throws IOException {
		SearchResponse response = client.prepareSearch("twitter", "searchdata")
		        .setQuery(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
		        .addAggregation(
		        	AggregationBuilders.terms("age_count").field("age")
		        	.order(BucketOrder.key(true)) //.order(BucketOrder.aggregation("avg_height", false))
		        	.subAggregation(
		        			AggregationBuilders.max("max_row_no").field("row"))
		        	.subAggregation(
		        			AggregationBuilders.min("min_row_no").field("row"))
		        	.subAggregation(
		        			AggregationBuilders.sum("sum_row_no").field("row"))
		        	.subAggregation(
		        			AggregationBuilders.avg("avg_row_no").field("row"))
		        	.subAggregation(
		        			AggregationBuilders.stats("stats_row_no").field("row"))
		        	.subAggregation(
		        			AggregationBuilders.extendedStats("ext_stats_row_no").field("row")) 
		        	.subAggregation(
		        			AggregationBuilders.percentiles("percent_row").field("row").percentiles(1.0, 5.0, 10.0, 20.0, 30.0, 75.0, 95.0, 99.0)) //求百分比值
		        	.subAggregation(
		        			AggregationBuilders.cardinality("cardinality_row").field("added_time").precisionThreshold(10000)) //count(distinct)去重
		        	.subAggregation(
		        			AggregationBuilders.topHits("top_row")
		        			.explain(true)
		        			.sort("row",  SortOrder.DESC)
		                    .size(2)
		                    .from(0)) //group rows 按照排序和size找到两个自己
		        	.subAggregation(
		        			AggregationBuilders.scriptedMetric("script_row")
		        			.initScript(new Script("state.transactions = []"))
		        			.mapScript(new Script("state.transactions.add(doc.age.value % 2 ==  1 ? doc.row.value : -1 * doc.row.value )"))
		        		    .combineScript(new Script("double profit = 0; for (t in state.transactions) { profit += t } return profit"))
		        		    .reduceScript(new Script("double profit = 0; for (a in states) { profit += a } return profit")) 
		        		    )
		        	.subAggregation(
		        			AggregationBuilders.histogram("histogram_age").field("row").interval(10) //步长分布
		        		    )
		        	.subAggregation(
		        			AggregationBuilders.filters("filters_age",
		        					new FiltersAggregator.KeyedFilter("odd", QueryBuilders.rangeQuery("row").from(12).to(15)), //按照条件聚合
		        		            new FiltersAggregator.KeyedFilter("odd2", QueryBuilders.rangeQuery("row").from(16).to(18)))
		        		    )
		        	.subAggregation(
		        			AggregationBuilders.significantTerms("significant_row").field("row"))
		        )
		        .setSize(10000)
		        .addSort("age", SortOrder.DESC)
		        .execute().actionGet();
		 Terms aggregation = response.getAggregations().get("age_count");
		 for (Terms.Bucket bucket : aggregation.getBuckets()) {
            System.out.println("age=" + bucket.getKey()+ ";age数量=" + bucket.getDocCount());
            Max  terms2 = bucket.getAggregations().get("max_row_no");
            Min  terms3 = bucket.getAggregations().get("min_row_no");
            Sum  terms4 = bucket.getAggregations().get("sum_row_no");
            Avg  terms5 = bucket.getAggregations().get("avg_row_no");
            Stats  terms6 = bucket.getAggregations().get("stats_row_no");
            ExtendedStats  terms7 = bucket.getAggregations().get("ext_stats_row_no");
            Percentiles  terms8 = bucket.getAggregations().get("percent_row");
            Cardinality   terms9 = bucket.getAggregations().get("cardinality_row");
            TopHits   terms10 = bucket.getAggregations().get("top_row");
            ScriptedMetric    terms11 = bucket.getAggregations().get("script_row");
            Histogram    terms12 = bucket.getAggregations().get("histogram_age");
            Filters terms13 = bucket.getAggregations().get("filters_age");
            SignificantTerms  terms14 = bucket.getAggregations().get("significant_row");
            System.out.println("添加时间=" + bucket.getKey() 
            		+ ";max_row_no=" 
            		+ terms2.getValue() 
            		+ ";min_row_no="
            		+ terms3.getValue()
            		+ ";sum_row_no="
            		+ terms4.getValue()
            		+ ";avg_row_no="
            		+ terms5.getValue()
            		+ ";stats_row_no="
            		+ JsonUtils.toJson(terms6)
            		+ ";ext_stats_row_no="
            		+ JsonUtils.toJson(terms7)
            		+ ";percent_row="
            		+ JsonUtils.toJson(terms8)
            		+ ";cardinality_row="
            		+ JsonUtils.toJson(terms9.getValue())
            		+ ";script_row="
            		+ JsonUtils.toJson(terms11.aggregation())
            		+ ";histogram_age="
            		+ JsonUtils.toJson(terms12.getBuckets())
            		+ ";histogram_age="
            		+ JsonUtils.toJson(terms12.getBuckets())
            	);
            
            for (Percentile entry : terms8) {
                double percent = entry.getPercent();    // Percent
                double value = entry.getValue();        // Value
                System.out.println("percent ["+percent+"], value ["+value+"]");
            }
            
            // We ask for top_hits for each bucket
            for (SearchHit hit : terms10.getHits().getHits()) {
            	System.out.println("top_row -> id ["+hit.getId()+"], _source ["+hit.getSourceAsString()+"]");
            }
            
            
            for (Filters.Bucket entry  : terms13.getBuckets()) {
            	System.out.println("filters_age -> key ["+entry.getKeyAsString()+"], doc_count ["+entry.getDocCount()+"]");
            }
            
            for (SignificantTerms.Bucket entry : terms14.getBuckets()) {
                System.out.println("SignificantTerms -> key ["+entry.getKeyAsString()+"], doc_count ["+entry.getDocCount()+"]");
            }
            
//            terms7.getVariance() 方差
//            terms7.getStdDeviation()  标准差
//            terms7.getSumOfSquares() 总平方和
//            terms7.getStdDeviationBound()
         }
	}
}

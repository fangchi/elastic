package com.fc.lucene;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.junit.Test;
import org.wltea.analyzer.cfg.Configuration;
import org.wltea.analyzer.lucene.IKAnalyzer;

import com.fc.basic.BaseTest;
import com.fc.basic.SearchTest;
import com.google.inject.util.Types;

import junit.framework.TestCase;

public class LuceneTest extends TestCase{
	
	private static String INDEX_DIR = "E:\\lucene\\luceneIndex";

	private Analyzer getIKAnalyzer(){
		org.elasticsearch.common.settings.Settings.Builder builder = Settings.builder();
		builder.put("path.home","E:\\Elastic\\Elasticsearch\\6.5.4\\plugins\\ik\\"); //需要将config文件拷贝至target才能运行
		builder.put("use_smart", "true");
		Settings settings = builder.build();
		Environment environment = new Environment(settings, PathUtils.get(System.getProperty("java.io.tmpdir")));
		Configuration configuration = new Configuration(environment, settings);
	    Analyzer analyzer = new IKAnalyzer(configuration);
	    Map<String, Analyzer> fieldAnalyzers = new HashMap<String, Analyzer>();
	    fieldAnalyzers.put("content", analyzer);
	    fieldAnalyzers.put("lineNumber", new KeywordAnalyzer());
	    PerFieldAnalyzerWrapper aWrapper = new PerFieldAnalyzerWrapper(analyzer,fieldAnalyzers);  
	    return aWrapper;
	}
	
	public void closeWriter(IndexWriter indexWriter) throws Exception {
		if (indexWriter != null) {
			indexWriter.close();
		}
	}
	
	public void cleanIndexFile(String filePath) throws Exception {
//		File indexFile = new File(filePath);
//		if (indexFile.exists()) {
//			indexFile.delete();
//		}
	}
	
	@Test
	public void testIndex() {
		URL url = SearchTest.class.getClassLoader().getResource("data.txt");
		File file = new File(url.getFile());
		List<String> contents = BaseTest.readFileByLines(file);
		IndexWriter indexWriter = null;
		Date date1 = new Date();
		FSDirectory directory = null;
		try {
			Analyzer analyzer = getIKAnalyzer();
			cleanIndexFile(INDEX_DIR);
			directory = FSDirectory.open(Paths.get(INDEX_DIR));
			File indexFile = new File(INDEX_DIR);
			if (!indexFile.exists()) {
				indexFile.mkdirs();
			}
			
			IndexWriterConfig config = new IndexWriterConfig(analyzer);
			//config.setCommitOnClose(false).setRAMBufferSizeMB(64);
			config.setInfoStream(new PrintStreamInfoStream(System.out));//打印
			config.setMaxBufferedDocs(30);
			config.setUseCompoundFile(false);//合并文件
			config.setSimilarity(new PayloadSimilarity());//可自实现排序算法
			//indexWriterConfig.setCodec(new Lucene54Codec(Mode.BEST_SPEED));  设置压缩算法
			indexWriter = new IndexWriter(directory, config);
			int rowNumber = 0;
			for (Iterator<String> iterator = contents.iterator(); iterator.hasNext();) {
				rowNumber++;
				String content = (String) iterator.next();
				Document document = new Document();
				// TextField 是一个会自动被索引和分词的字段。一般被用在文章的正文部分
				document.add(new StringField("lineNumber", String.valueOf(rowNumber), Store.YES));///YES 可以搜索，保存原值
				document.add(new StringField("lineNumber2", String.valueOf(rowNumber), Store.NO));///NO 可以搜索，不保存原值
				//大小,数字类型使用point添加到索引中,同时如果需要存储,由于没有Stroe,所以需要再创建一个StoredField进行存储
				document.add(new LongPoint("age",rowNumber));
				document.add(new StoredField("age",rowNumber));
				
				//同时添加排序支持
				document.add(new NumericDocValuesField("age",rowNumber));
				
				
				document.add(new LongPoint("rank",rowNumber%10));
				document.add(new StoredField("rank",rowNumber%10));
				document.add(new NumericDocValuesField("rank",rowNumber%10));
				/**
				 * If a document is indexed but not stored, you can search for it, but it won't be returned with search results.
				 * One reasonably common pattern is to use lucene for search, but only have an ID field being stored which can be used to retrieve the full contents of the document/record from, 
				 * for instance, a SQL database, a file system, or an web resource.
				 */
				//StringField会被索引，但是不会被分词，即会被当作一个完整的token处理，一般用在“国家”或者“ID
				Field field = new TextField("content", content, Store.YES);
				document.add(field);
				document.add(new StoredField("content",rowNumber));
				long sequence_numbers = indexWriter.addDocument(document);
				 System.out.println("_____________________sequence_numbers:"+sequence_numbers);
			}
			indexWriter.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(indexWriter!=null){
				try {
					closeWriter(indexWriter);
					if(directory!=null){
						directory.close();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			Date date2 = new Date();
	        System.out.println("构建索引-----耗时：" + (date2.getTime() - date1.getTime()) + "ms\n");
		}
	}
	
	 /**
     * 查找索引，返回符合条件的文件
     * @param text 查找的字符串
     * @return 符合条件的文件List
     */
	@Test
    public void testSearchIndex(){
		String searchText = "昆仑右手";
        Date date1 = new Date();
        try{
        	Directory directory = FSDirectory.open(Paths.get(INDEX_DIR));
        	Analyzer analyzer =  getIKAnalyzer();
            DirectoryReader ireader = DirectoryReader.open(directory);
            IndexSearcher isearcher = new IndexSearcher(ireader);
            isearcher.setSimilarity(new ClassicSimilarity());
            // QueryParser的第一个参数就是创建index的Field，我们的例子中filename content path
            QueryParser parser = new QueryParser("content", analyzer);
            Query query = parser.parse(searchText);
            ScoreDoc[] hits = isearcher.search(query, 20).scoreDocs;
            printHits(isearcher,query,hits);
            ireader.close();
            directory.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        Date date2 = new Date();
        System.out.println("查看索引-----耗时：" + (date2.getTime() - date1.getTime()) + "ms\n");
    }
	
	
	@Test
    public void testSearchIndexTermQuery(){
        Date date1 = new Date();
        try{
        	Directory directory = FSDirectory.open(Paths.get(INDEX_DIR));
            DirectoryReader ireader = DirectoryReader.open(directory);
            IndexSearcher isearcher = new IndexSearcher(ireader);
            isearcher.setSimilarity(new ClassicSimilarity());
            //TermQuery，通过项查询，TermQuery不使用分析器所以建议匹配不分词的Field域查询，比如订单号、分类ID号等
            Query query = new TermQuery(new Term("lineNumber","1"));
            ScoreDoc[] hits = isearcher.search(query, 20).scoreDocs;
            printHits(isearcher,query,hits);
            ireader.close();
            directory.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        Date date2 = new Date();
        System.out.println("查看索引-----耗时：" + (date2.getTime() - date1.getTime()) + "ms\n");
    }
	
	@Test
    public void testSearchIndexWildcardQuery(){
        Date date1 = new Date();
        try{
        	Directory directory = FSDirectory.open(Paths.get(INDEX_DIR));
            DirectoryReader ireader = DirectoryReader.open(directory);
            IndexSearcher isearcher = new IndexSearcher(ireader);
            isearcher.setSimilarity(new ClassicSimilarity());
            /**********WildcardQuery通配符。可以使用'*'或者‘？’**************/
            Term term = new Term("lineNumber", "1?");
            Query query = new WildcardQuery(term);
            
            ScoreDoc[] hits = isearcher.search(query, 20).scoreDocs;
            printHits(isearcher,query,hits);
            ireader.close();
            directory.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        Date date2 = new Date();
        System.out.println("查看索引-----耗时：" + (date2.getTime() - date1.getTime()) + "ms\n");
    }
	
	@Test
    public void testSearchIndexRangeQuery(){
        Date date1 = new Date();
        try{
        	Directory directory = FSDirectory.open(Paths.get(INDEX_DIR));
            DirectoryReader ireader = DirectoryReader.open(directory);
            IndexSearcher isearcher = new IndexSearcher(ireader);
            isearcher.setSimilarity(new ClassicSimilarity());
            //12~14
            Query query=LongPoint.newRangeQuery("age", 12L, 14L);
            
            ScoreDoc[] hits = isearcher.search(query, 20).scoreDocs;
            printHits(isearcher,query,hits);
            ireader.close();
            directory.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        Date date2 = new Date();
        System.out.println("查看索引-----耗时：" + (date2.getTime() - date1.getTime()) + "ms\n");
    }
	
	@Test
    public void testSearchIndexBooleanQuery(){
        Date date1 = new Date();
        try{
        	Directory directory = FSDirectory.open(Paths.get(INDEX_DIR));
            DirectoryReader ireader = DirectoryReader.open(directory);
            IndexSearcher isearcher = new IndexSearcher(ireader);
            isearcher.setSimilarity(new ClassicSimilarity());
            //组合条件
            Query query1=new TermQuery(new Term("content","平头"));
            Query query2=new TermQuery(new Term("content","右手"));
            //相当于一个包装类，将 Query 设置 Boost 值 ，然后包装起来。
            //再通过复合查询语句，可以突出 Query 的优先级
            BoostQuery query=new BoostQuery(query2, 20f);
            //创建BooleanQuery.Builder
            BooleanQuery.Builder  builder=new BooleanQuery.Builder();
            //添加逻辑
            /**
             *   1．MUST和MUST：取得两个查询子句的交集。  and
                 2．MUST和MUST_NOT：表示查询结果中不能包含MUST_NOT所对应得查询子句的检索结果。
                 3．SHOULD与MUST_NOT：连用时，功能同MUST和MUST_NOT。
                 4．SHOULD与MUST连用时，结果为MUST子句的检索结果,但是SHOULD可影响排序。
                 5．SHOULD与SHOULD：表示“或”关系，最终检索结果为所有检索子句的并集。
                 6．MUST_NOT和MUST_NOT：无意义，检索无结果。
             */
            builder.add(query1, Occur.SHOULD);// 文件名不包含词语,但是内容必须包含姚振
            builder.add(query, Occur.SHOULD);
            //build query
            BooleanQuery  booleanQuery=builder.build();
            ScoreDoc[] hits = isearcher.search(booleanQuery, 20).scoreDocs;
            printHits(isearcher,booleanQuery,hits);
            ireader.close();
            directory.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        Date date2 = new Date();
        System.out.println("查看索引-----耗时：" + (date2.getTime() - date1.getTime()) + "ms\n");
    }
	
	@Test
    public void testSearchIndexAllQuery(){
        Date date1 = new Date();
        try{
        	Directory directory = FSDirectory.open(Paths.get(INDEX_DIR));
            DirectoryReader ireader = DirectoryReader.open(directory);
            IndexSearcher isearcher = new IndexSearcher(ireader);
            isearcher.setSimilarity(new ClassicSimilarity());
            Query queryAll=new MatchAllDocsQuery();
            ScoreDoc[] hits = isearcher.search(queryAll, 20).scoreDocs;
            printHits(isearcher,queryAll,hits);
            ireader.close();
            directory.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        Date date2 = new Date();
        System.out.println("查看索引-----耗时：" + (date2.getTime() - date1.getTime()) + "ms\n");
    }
	
	@Test
    public void testSearchPrefixQuery(){
        Date date1 = new Date();
        try{
        	Directory directory = FSDirectory.open(Paths.get(INDEX_DIR));
            DirectoryReader ireader = DirectoryReader.open(directory);
            IndexSearcher isearcher = new IndexSearcher(ireader);
            isearcher.setSimilarity(new ClassicSimilarity());
            Query query=new PrefixQuery(new Term("content","何足道"));
            ScoreDoc[] hits = isearcher.search(query, 20).scoreDocs;
            printHits(isearcher,query,hits);
            ireader.close();
            directory.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        Date date2 = new Date();
        System.out.println("查看索引-----耗时：" + (date2.getTime() - date1.getTime()) + "ms\n");
    }
	
	
	@Test
    public void testSearchFuzzyQuery(){
        Date date1 = new Date();
        try{
        	Directory directory = FSDirectory.open(Paths.get(INDEX_DIR));
            DirectoryReader ireader = DirectoryReader.open(directory);
            IndexSearcher isearcher = new IndexSearcher(ireader);
            isearcher.setSimilarity(new ClassicSimilarity());
            Query query=new FuzzyQuery(new Term("content","何手道"));
            ScoreDoc[] hits = isearcher.search(query, 20).scoreDocs;
            printHits(isearcher,query,hits);
            ireader.close();
            directory.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        Date date2 = new Date();
        System.out.println("查看索引-----耗时：" + (date2.getTime() - date1.getTime()) + "ms\n");
    }
	
	
	@Test
    public void testSearchFunctionScoreQuery(){
        Date date1 = new Date();
        try{
        	Directory directory = FSDirectory.open(Paths.get(INDEX_DIR));
            DirectoryReader ireader = DirectoryReader.open(directory);
            IndexSearcher isearcher = new IndexSearcher(ireader);
            isearcher.setSimilarity(new ClassicSimilarity());
            Query query1=new TermQuery(new Term("content","昆仑"));
            FunctionQuery functionQuery = new FunctionQuery(new RankValueSource("linenumber")); 
            CustomScoreQuery customScoreQuery = new CustomScoreQuery(query1, functionQuery);
            //SortField  contentSortField = new SortField("linenumber", Type.DOC);//doc排序
            SortField  contentSortField = new SortField(null, Type.SCORE); //评分排序
            Sort sort = new Sort(new SortField[] {contentSortField});
            ScoreDoc[] hits = isearcher.search(customScoreQuery, 10,sort,true,false).scoreDocs;
            printHits(isearcher,customScoreQuery,hits);
            ireader.close();
            directory.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        Date date2 = new Date();
        System.out.println("查看索引-----耗时：" + (date2.getTime() - date1.getTime()) + "ms\n");
    }
	
	
//	@Test
//    public void testSearchFunctionScoreQuery2(){
//        Date date1 = new Date();
//        try{
//        	Directory directory = FSDirectory.open(Paths.get(INDEX_DIR));
//            DirectoryReader ireader = DirectoryReader.open(directory);
//            IndexSearcher isearcher = new IndexSearcher(ireader);
//            isearcher.setSimilarity(new ClassicSimilarity());
//            Query query1=new TermQuery(new Term("content","昆仑"));
//            FunctionScoreQuery customScoreQuery = FunctionScoreQuery.boostByValue(query1,new FCDoubleValuesSource());
//            //SortField  contentSortField = new SortField("linenumber", Type.DOC);//doc排序
//            SortField  contentSortField = new SortField(null, Type.SCORE); //评分排序
//            Sort sort = new Sort(new SortField[] {contentSortField});
//            ScoreDoc[] hits = isearcher.search(customScoreQuery, 10,sort).scoreDocs;
//            printHits(isearcher,query1,hits);
//            ireader.close();
//            directory.close();
//        }catch(Exception e){
//            e.printStackTrace();
//        }
//        Date date2 = new Date();
//        System.out.println("查看索引-----耗时：" + (date2.getTime() - date1.getTime()) + "ms\n");
//    }
	
	private void printHits(IndexSearcher isearcher,Query query ,ScoreDoc[] hits) throws IOException, InvalidTokenOffsetsException{
		System.out.println("查询结果条数:"+hits.length); 
		QueryScorer scorer=new QueryScorer(query); 
		 //显示得分高的片段 
	     Fragmenter fragmenter=new SimpleSpanFragmenter(scorer); 
	     //设置标签内部关键字的颜色 
	     //第一个参数：标签的前半部分；第二个参数：标签的后半部分。 
	     SimpleHTMLFormatter simpleHTMLFormatter=new SimpleHTMLFormatter("<b><font color='red'>","</font></b>");        
	     //第一个参数是对查到的结果进行实例化；第二个是片段得分（显示得分高的片段，即摘要） 
	     Highlighter highlighter=new Highlighter(simpleHTMLFormatter, scorer); 
	     //设置片段 
	     highlighter.setTextFragmenter(fragmenter); 
		for (int i = 0; i < hits.length; i++) {
            Document hitDoc = isearcher.doc(hits[i].doc);
            System.out.println("____________________________");
            System.out.println("docId:" + hits[i].doc);
            System.out.println("content:" + hitDoc.get("content"));
            System.out.println("contentHight:" + highlighter.getBestFragment(getIKAnalyzer(), "content", hitDoc.get("content")));
            System.out.println("lineNumber:" + hitDoc.get("lineNumber"));
            System.out.println("lineNumber2:" + hitDoc.get("lineNumber2"));
            System.out.println("age:" + hitDoc.get("age"));
            System.out.println("score:" +hits[i].score);
            System.out.println(isearcher.explain(query, hits[i].doc));
            System.out.println("____________________________");
        }
	}
}

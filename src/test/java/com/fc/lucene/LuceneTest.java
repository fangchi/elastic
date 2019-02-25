package com.fc.lucene;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
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
	    return analyzer;
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
		try {
			Analyzer analyzer = getIKAnalyzer();
			cleanIndexFile(INDEX_DIR);
			FSDirectory directory = FSDirectory.open(Paths.get(INDEX_DIR));
			File indexFile = new File(INDEX_DIR);
			if (!indexFile.exists()) {
				indexFile.mkdirs();
			}
			
			IndexWriterConfig config = new IndexWriterConfig(analyzer);
			//config.setCommitOnClose(false).setRAMBufferSizeMB(64);
			config.setInfoStream(new PrintStreamInfoStream(System.out));//打印
			config.setMaxBufferedDocs(30);
			config.setUseCompoundFile(false);//合并文件
			config.setSimilarity(new ClassicSimilarity());//可自实现排序算法
			indexWriter = new IndexWriter(directory, config);
			int rowNumber = 0;
			for (Iterator<String> iterator = contents.iterator(); iterator.hasNext();) {
				rowNumber++;
				String content = (String) iterator.next();
				Document document = new Document();
				// TextField 是一个会自动被索引和分词的字段。一般被用在文章的正文部分
				document.add(new StringField("lineNumber", String.valueOf(rowNumber), Store.YES));
				//StringField会被索引，但是不会被分词，即会被当作一个完整的token处理，一般用在“国家”或者“ID
				document.add(new TextField("content", content, Store.YES));
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
		String text = "昆仑";
        Date date1 = new Date();
        try{
        	Directory directory = FSDirectory.open(Paths.get(INDEX_DIR));
        	Analyzer analyzer =  getIKAnalyzer();
            DirectoryReader ireader = DirectoryReader.open(directory);
            IndexSearcher isearcher = new IndexSearcher(ireader);
 
            // QueryParser的第一个参数就是创建index的Field，我们的例子中filename content path
            QueryParser parser = new QueryParser("content", analyzer);
            Query query = parser.parse(text);
            ScoreDoc[] hits = isearcher.search(query, 20, Sort.INDEXORDER).scoreDocs;
            System.out.println(hits.length);
 
            for (int i = 0; i < hits.length; i++) {
                Document hitDoc = isearcher.doc(hits[i].doc);
                System.out.println("____________________________");
                System.out.println(hitDoc.get("content"));
                System.out.println(hitDoc.get("lineNumber"));
                System.out.println("____________________________");
            }
            ireader.close();
            directory.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        Date date2 = new Date();
        System.out.println("查看索引-----耗时：" + (date2.getTime() - date1.getTime()) + "ms\n");
    }
}

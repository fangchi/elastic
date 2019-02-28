package com.fc.lucene;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.CustomScoreProvider;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.search.Query;

public class MyCustomScoreQuery extends CustomScoreQuery {

	public MyCustomScoreQuery(Query subQuery) {
		super(subQuery);
	}
    
    protected CustomScoreProvider getCustomScoreProvider(LeafReaderContext context) throws IOException {
    	// 默认情况评分是根据原有的评分*传入进来的评分
        //return super.getCustomScoreProvider(reader);
        return new MyCustomScoreProvider(context);
     }

}


class MyCustomScoreProvider extends CustomScoreProvider {

    public MyCustomScoreProvider(LeafReaderContext context) {
		super(context);
	}

    @Override
    public float customScore(int doc, float subQueryScore, float valSrcScore)
            throws IOException {
    	String rank = this.context.reader().document(doc).get("rank");
    	String lineNumber = this.context.reader().document(doc).get("lineNumber");
    	if("66".equals(lineNumber)){
    		System.out.println();
    	}
    	Long rankVal = Long.parseLong(rank)+1;
    	return super.customScore(doc, subQueryScore, valSrcScore  * rankVal) ;
    }
}

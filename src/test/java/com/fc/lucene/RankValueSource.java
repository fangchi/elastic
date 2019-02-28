package com.fc.lucene;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.valuesource.FieldCacheSource;

public class RankValueSource extends FieldCacheSource{

	public RankValueSource(String field) {
		super(field);
	}

	/**
	 * 这里Map里存的是IndexSeacher,context.get("searcher");获取
	 */
	@Override
	public FunctionValues getValues(Map context, LeafReaderContext leafReaderContext) throws IOException {
		return new FunctionValues() {
			@Override
			public float floatVal(int doc) throws IOException {
				String rank = leafReaderContext.reader().document(doc).get("rank");
				return Long.parseLong(rank)+1;
			}

			@Override
			public int intVal(int doc) throws IOException {
				String rank = leafReaderContext.reader().document(doc).get("rank");
				return Integer.parseInt(rank)+1;
			}

			@Override
			public String toString(int doc) throws IOException {
				return description() + " RankValueSource.intVal=" + intVal(doc);
			}
		};
	}

}

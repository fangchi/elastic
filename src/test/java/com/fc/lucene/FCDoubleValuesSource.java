package com.fc.lucene;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;

public class FCDoubleValuesSource extends DoubleValuesSource{

	@Override
	public boolean isCacheable(LeafReaderContext ctx) {
		return false;
	}

	@Override
	public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
		return scores;
	}

	@Override
	public boolean needsScores() {
		return true;
	}

	@Override
	public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
		return null;
	}

	@Override
	public int hashCode() {
		return 0;
	}

	@Override
	public boolean equals(Object obj) {
		return false;
	}

	@Override
	public String toString() {
		return null;
	}

}

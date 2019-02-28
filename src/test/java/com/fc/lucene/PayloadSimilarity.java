package com.fc.lucene;

import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.util.BytesRef;

public class PayloadSimilarity extends ClassicSimilarity {

	@Override
	public float scorePayload(int doc, int start, int end, BytesRef payload) {
		return PayloadHelper.decodeFloat(payload.bytes);
	}
}

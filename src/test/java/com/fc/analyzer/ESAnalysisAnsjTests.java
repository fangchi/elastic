package com.fc.analyzer;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.ansj.elasticsearch.index.config.AnsjElasticConfigurator;
import org.ansj.elasticsearch.plugin.AnalysisAnsjPlugin;
import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.junit.Test;

public class ESAnalysisAnsjTests {

	@Test
    public void testDefaultsIcuAnalysis() throws InterruptedException, ExecutionException {
//    	AnalysisAnsjPlugin  analysisAnsjPlugin=   new AnalysisAnsjPlugin();
//    	Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>>  map = analysisAnsjPlugin.getAnalyzers();
    	org.elasticsearch.common.settings.Settings.Builder builder = Settings.builder();
    	builder.put("path.home","E:\\tcrm\\elasticsearch-analysis-ansj\\config"); //需要将config文件拷贝至target才能运行
		Settings settings = builder.build();
		Environment environment = new Environment(settings, PathUtils.get(System.getProperty("java.io.tmpdir")));
		AnsjElasticConfigurator ansjElasticConfigurator = new AnsjElasticConfigurator(environment);
    	//map.get("index_ansj").get(environment, name);
    	System.out.println();
    }
}

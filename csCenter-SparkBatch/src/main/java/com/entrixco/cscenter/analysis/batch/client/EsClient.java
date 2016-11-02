package com.entrixco.cscenter.analysis.batch.client;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.DataFrame;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsClient {
	
	private static final Logger logger = LoggerFactory.getLogger(EsClient.class);
	
	public static void saveES(DataFrame df, String esIndex) {
		DataFrame newdf = df.drop("dt").drop("hh");
		JavaEsSparkSQL.saveToEs(newdf, esIndex);
	}
	
	//for index with unique id
	public static void saveES(DataFrame df, String esIndex, String idField) {
		Map<String, String> mmap = new HashMap<>();
		mmap.put("es.mapping.id", idField);
		DataFrame newdf = df.drop("dt").drop("hh");
		JavaEsSparkSQL.saveToEs(newdf, esIndex, mmap);
	}
	
	private Client esClient;
	private String esCluster, esNodes;
	
	public EsClient(Map<String, Object> cmap) {
		esCluster = (String)cmap.get("default.es.cluster");
		esNodes = (String)cmap.get("default.es.nodes");
	}
	
	public void open() {
		if(esClient!=null) return;
		
		ESLoggerFactory.getRootLogger().setLevel("INFO");
		Settings settings = Settings.settingsBuilder().put("cluster.name", esCluster).build();
		TransportClient tc = TransportClient.builder().settings(settings).addPlugin(DeleteByQueryPlugin.class).build();
		try {
			for(String node : esNodes.split(",")) {
				InetAddress nodeaddr = InetAddress.getByName(node.split(":")[0]);
				tc.addTransportAddress(new InetSocketTransportAddress(nodeaddr, 9300));
				logger.info("Add Elastic Node : ["+node+"]");
			}
		} catch(Exception e) {
			logger.error("esClient open", e);
			throw new RuntimeException(e);
		}
		this.esClient = tc;
	}
	
	public SearchResponse search(String index, String type, String query, int size, String sorts) {
		 SearchRequestBuilder builder = esClient.prepareSearch(index).setTypes(type)
				.setQuery(QueryBuilders.queryStringQuery(query))
				.setSize(size);
		 for(String sort : sorts.split(",")) {
			 String[] orders = sort.split(":");
			 builder.addSort(orders[0], orders[1].equals("desc") ? SortOrder.DESC : SortOrder.ASC);
		 }
		 return builder.execute().actionGet();
	}
	
	public void delete(String index, String type, String smin, String emin){
		logger.info(">>>>>>>Delete ES index: {}, smin: {}, emin: {}",index,smin,emin);
		
		StringBuilder b = new StringBuilder("");
	    b.append("{");
	    b.append("  \"query\": {");  
	    b.append("      \"range\": {");
	    b.append("          \"stat_min\": { \"gte\":\""+smin+"\", \"lte\":\""+emin+"\" }"  );
	    b.append("      }");
	    b.append("  }");
	    b.append("}");
		DeleteByQueryResponse response = new DeleteByQueryRequestBuilder(esClient, DeleteByQueryAction.INSTANCE)
				.setIndices(index).setTypes(type).setSource(b.toString()).execute().actionGet();
		logger.info(">>>>>>>"+response.toString());
	}
	
	public void close() {esClient.close();}

}

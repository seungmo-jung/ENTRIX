package com.entrixco.cscenter.analysis.streaming.parser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import scala.Tuple2;

public class NetworkParser extends StreamParser{
	
	private static final Logger logger = LoggerFactory.getLogger(NetworkParser.class);
	
	public static void parse(final HashMap<String, Object> configMap,JavaStreamingContext jstCtx) {
		
		String topics = (String)configMap.get("colNetTopic");
		String brokers = (String)configMap.get("brokers");
		String zookeeper = (String)configMap.get("zookeeper");
		String groupid = (String)configMap.get("sparkGroupID");
		int numThreads = Integer.parseInt((String)configMap.get("numThreads"));
		
//		JavaPairInputDStream<String, String> pairstream = createKafkaDStream(topics,brokers,jstCtx);
//		JavaPairInputDStream<String, String> pairstream = createKafkaDStream2(topics,brokers,groupid,jstCtx,zookeeper);
		JavaPairInputDStream<String, String> pairstream = createKafkaStream(jstCtx,zookeeper,groupid,topics,numThreads);
		
		
		JavaPairDStream<String, String> fsnmp=pairstream.filter(new Function<Tuple2<String, String>, Boolean>() {
			public Boolean call(Tuple2<String, String> t2) {
				String data = subData(t2._2,"[");
				JsonObject json = new JsonParser().parse(data).getAsJsonObject();
				String plugin = json.get("plugin").getAsString();
				
				return plugin.equals("snmp");
			}
		});
		
		JavaDStream<Row> linestream = fsnmp.map(new Function<Tuple2<String, String>, Row>() {
			public Row call(Tuple2<String, String> t2) {
				Object[] rt = null;
				try {
					String data = subData(t2._2,"[");
					JsonObject json = new JsonParser().parse(data).getAsJsonObject();
					String values = json.get("values").getAsJsonArray().toString();
					values = subData(values,"[");
					String dstypes = json.get("dstypes").getAsJsonArray().toString();
					dstypes = subData(dstypes,"[").replaceAll("\"", "");
					String dsnames = json.get("dsnames").getAsJsonArray().toString();
					dsnames = subData(dsnames,"[").replaceAll("\"", "");
					String time = setTime(json.get("time").getAsLong());
					String interval = json.get("interval").getAsString();
					String host = json.get("host").getAsString();
					String plugin = json.get("plugin").getAsString();
					String plugin_instance = json.has("plugin_instance") ? json.get("plugin_instance").getAsString():"";
					String type = json.get("type").getAsString();
					String type_instance = json.get("type_instance").getAsString();
					String meta = json.has("meta") ? json.get("meta").getAsString():"";
					String dt = time.substring(0,10).replace("-",""); 
					String hh = time.substring(11,13);
					rt= new String[]{values,dstypes,dsnames,time,interval,host,plugin,plugin_instance,type,type_instance,meta,dt,hh};
				} 
				catch (Exception e) {
					e.printStackTrace();
				}
//				logger.info(">>>>> vals = {}",Arrays.toString(rt));
				return RowFactory.create(rt);
			}
		});
		
		String columns[] = configMap.get("colColumns").toString().split(",");
		String nettable = (String)configMap.get("colNetTable");
		HiveContext hiveCtx = (HiveContext)configMap.get("hiveCtx");
		
		saveTable(linestream,nettable,columns,hiveCtx);
		
	}

}

package com.entrixco.cscenter.analysis.streaming.parser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.curator.framework.CuratorFramework;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import scala.Tuple2;

public class NetworkParser2 extends StreamParser{
	
	private static final Logger logger = LoggerFactory.getLogger(NetworkParser2.class);
	
	public static void parse(final HashMap<String, Object> configMap,JavaStreamingContext jstCtx) {
		
		String topics = (String)configMap.get("colNetTopic");
		String brokers = (String)configMap.get("brokers");
		String groupid = (String)configMap.get("sparkGroupID");
		CuratorFramework  curatorFramework = (CuratorFramework)configMap.get("curatorFramework");
		
		JavaDStream<String> pairstream = createKafkaDStream(topics, brokers, groupid, jstCtx, curatorFramework);
		
		final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
		JavaDStream<String> trans = pairstream.transform(new Function<JavaRDD<String>,JavaRDD<String>>(){
			public JavaRDD<String> call(JavaRDD<String> rdd){
				OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
				offsetRanges.set(offsets);
				return rdd;
			}
		});
		
		JavaDStream<String> fsnmp=trans.filter(new Function<String, Boolean>() {
			public Boolean call(String t2) {
//				String data = subData(t2.substring(101),"[");
				String data = subData(t2,"[");
				JsonObject json = new JsonParser().parse(data).getAsJsonObject();
				String plugin = json.get("plugin").getAsString();
				String type = json.get("type_instance").getAsString();
				
				return plugin.equals("snmp")&&!type.startsWith("Link");
			}
		});
		
		JavaDStream<Row> linestream = fsnmp.map(new Function<String, Row>() {
			public Row call(String t2) {
//				logger.info(""+t2);
				Object[] rt = null;
				try {
//					String data = subData(t2.substring(101),"[");
					String data = subData(t2,"[");
					JsonObject json = new JsonParser().parse(data).getAsJsonObject();
					String values = json.get("values").getAsJsonArray().toString();
					values = subData(values,"[");
//					logger.info(data);
					String dstypes = json.get("dstypes").getAsJsonArray().toString();
					dstypes = subData(dstypes,"[").replaceAll("\"", "");
					String dsnames = json.get("dsnames").getAsJsonArray().toString();
					dsnames = subData(dsnames,"[").replaceAll("\"", "");
					String time = setTime(json.get("time").getAsLong());
					String interval = json.get("interval").getAsString();
					String host = json.get("host").getAsString().toUpperCase();
					String plugin = json.get("plugin").getAsString();
					String type = json.get("type").getAsString();
					String type_instance = json.get("type_instance").getAsString();
					String dt = time.substring(0,10).replace("-",""); 
					String hh = time.substring(11,13);
					rt= new String[]{values,dstypes,dsnames,time,interval,host,plugin,type,type_instance,dt,hh};
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
		
		saveTable(linestream,nettable,columns,hiveCtx, groupid, curatorFramework,offsetRanges);
		
	}

}

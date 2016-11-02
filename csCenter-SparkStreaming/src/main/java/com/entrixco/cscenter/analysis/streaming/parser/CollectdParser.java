package com.entrixco.cscenter.analysis.streaming.parser;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class CollectdParser extends StreamParser {
	
	private static final Logger logger = LoggerFactory.getLogger(CollectdParser.class);
	
	public static void parse(final HashMap<String, Object> configMap,JavaStreamingContext jstCtx) {
		
		String topics = (String)configMap.get("colTopic");
		String brokers = (String)configMap.get("brokers");
		String zookeeper = (String)configMap.get("zookeeper");
		String groupid = (String)configMap.get("sparkGroupID");
		int numThreads = Integer.parseInt((String)configMap.get("numThreads"));
		
//		JavaPairInputDStream<String, String> pairstream = createKafkaDStream(topics,brokers,jstCtx);
//		JavaPairInputDStream<String, String> pairstream = createKafkaDStream2(topics,brokers,groupid,jstCtx,zookeeper);
		JavaPairInputDStream<String, String> pairstream = createKafkaStream(jstCtx,zookeeper,groupid,topics,numThreads);
		
		JavaDStream<Row> linestream = pairstream.map(new Function<Tuple2<String, String>, Row>() {
			public Row call(Tuple2<String, String> t2) {
				Object[] rt = null;
				try {
					String data = t2._2();
					if(!data.startsWith("[")){
						data=data.substring(15);
					}
					JSONObject json = new JSONObject(data.substring(1, data.length()-1));
					String values = subData(json.getString("values"),"[");
					String dstypes = subData(json.getString("dstypes").replace("\"", ""),"[");
					String dsnames = subData(json.getString("dsnames").replace("\"", ""),"[");
					String time = setTime(json.getLong("time"));
					String interval = json.getString("interval");
					String host = json.getString("host");
					String plugin = json.getString("plugin");
					String plugin_instance = json.has("plugin_instance") ? json.getString("plugin_instance"):"";
					String type = json.getString("type");
					String type_instance = json.getString("type_instance");
					String meta = json.has("meta") ? json.getString("meta"):"";
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
		}).persist();
		
		JavaDStream<Row> cpust = typeFilter(linestream,"cpu");
		JavaDStream<Row> memst = typeFilter(linestream,"memory");
		JavaDStream<Row> diskst = typeFilter(linestream,"df");
		
		String columns[] = configMap.get("colColumns").toString().split(",");
		String cputable = (String)configMap.get("colCpuTable");
		String memtable = (String)configMap.get("colMemTable");
		String disktable = (String)configMap.get("colDiskTable");
		HiveContext hiveCtx = (HiveContext)configMap.get("hiveCtx");
		
		saveTable(cpust,cputable,columns,hiveCtx);
		saveTable(memst,memtable,columns,hiveCtx);
		saveTable(diskst,disktable,columns,hiveCtx);
		
	}
	
	public static JavaDStream<Row> typeFilter(JavaDStream<Row> linestream,final String type){
		JavaDStream<Row> filterds = linestream.filter(new Function<Row, Boolean>() {
			public Boolean call(Row row) {
				return row.getString(6).equals(type)||row.getString(8).equals(type);
			}
		});
		return filterds;
	}

}

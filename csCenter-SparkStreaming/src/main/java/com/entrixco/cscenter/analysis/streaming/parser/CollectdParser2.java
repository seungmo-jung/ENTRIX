package com.entrixco.cscenter.analysis.streaming.parser;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectdParser2 extends StreamParser {
	
	private static final Logger logger = LoggerFactory.getLogger(CollectdParser2.class);
	
	public static void parse(final HashMap<String, Object> configMap,JavaStreamingContext jstCtx) {
		
		String topics = (String)configMap.get("colTopic");
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
		
		JavaDStream<Row> linestream = trans.map(new Function<String, Row>() {
			public Row call(String t2) {
				Object[] rt = null;
				try {
					String data = t2;
//					logger.info(""+t2);
					if(!data.startsWith("[")){
						data=data.substring(15);
					}
					JSONObject json = new JSONObject(data.substring(1, data.length()-1));
					String values = subData(json.getString("values"),"[");
					String dstypes = subData(json.getString("dstypes").replace("\"", ""),"[");
					String dsnames = subData(json.getString("dsnames").replace("\"", ""),"[");
					String time = setTime(json.getLong("time"));
					String interval = json.getString("interval");
					String host = json.getString("host").toUpperCase();
					String plugin = json.getString("plugin");
					String plugin_instance = json.has("plugin_instance") ? json.getString("plugin_instance"):"";
					if(plugin.equals("df"))	plugin_instance = plugin_instance.replace("-", "/");
					String type = json.getString("type");
					String type_instance = json.getString("type_instance");
					String meta = json.has("meta") ? json.getString("meta"):"";
					String dt = time.substring(0,10).replace("-","");
					String hh = time.substring(11,13);
					
//					if(plugin.equals("memory") && type_instance.equals("used")){
//						return RowFactory.create(new String[0]);
//					}else if(plugin.equals("memory") && type_instance.equals("free")) {
//						values = String.valueOf(100-Double.parseDouble(values));
//						type_instance = "used";
//					}
					
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
		saveTable(diskst,disktable,columns,hiveCtx,groupid,curatorFramework,offsetRanges);
	}
	
	public static JavaDStream<Row> typeFilter(JavaDStream<Row> linestream,final String type){
		JavaDStream<Row> filterds = linestream.filter(new Function<Row, Boolean>() {
			public Boolean call(Row row) {
				if(type.equals("cpu")){
					return row.length()>0 && (row.getString(6).equals(type)||row.getString(8).equals(type))
							&& (row.getString(9).equals("idle")||row.getString(9).equals("system")||row.getString(9).equals("user"));
				}else if(type.equals("memory")){
					return row.length()>0 && (row.getString(6).equals(type)||row.getString(8).equals(type))
							&& row.getString(9).equals("used");
				}else if(type.equals("df")){
					return row.length()>0 && (row.getString(6).equals(type)||row.getString(8).equals(type))
							&& (row.getString(9).equals("free")||row.getString(9).equals("used"));
				}else{
					return row.length()>0 && (row.getString(6).equals(type)||row.getString(8).equals(type));
				}
			}
		});
		return filterds;
	}

}

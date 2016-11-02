package com.entrixco.cscenter.analysis.streaming.parser;

import java.util.Arrays;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class SedParser2 extends StreamParser{
	
	private static final Logger logger = LoggerFactory.getLogger(SedParser2.class);
	
	public static void parse(final HashMap<String, Object> configMap,JavaStreamingContext jstCtx) {
		
		String topics = (String)configMap.get("sedTopic");
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
//				logger.info(""+t2);
				JsonObject json = new JsonParser().parse(t2).getAsJsonObject();
				String line = json.get("message").getAsString();
//				String line = t2;
//				logger.info(">>>>>> {}",line);
				String ar[] = line.split(",");
				for(int i=0;i<ar.length;i++){
					ar[i] = ar[i].trim();
				}
				ar[1]=subData(ar[1],"\"");
				ar[4]=subData(ar[4],"{");

				String dt = ar[1].substring(0,10).replace("-","");
				String hh = ar[1].substring(11,13);
				String sar = Arrays.toString(ar).replace(", ", ",");
				String[] ra = (sar.substring(1, sar.length()-1)+","+dt+","+hh).split(",");
//				logger.info(">>>>> vals = {}",Arrays.toString(ra).replace(", ", ","));
				
				return RowFactory.create(ra);
			}
		});
		
		String tablename = configMap.get("sedTable").toString();
		String[] columns = configMap.get("sedColumns").toString().split(",");
		HiveContext hiveCtx = (HiveContext)configMap.get("hiveCtx");
		
		saveTable(linestream,tablename,columns,hiveCtx, groupid, curatorFramework,offsetRanges);
//		saveTable(linestream,tablename,columns,hiveCtx);
	}
}

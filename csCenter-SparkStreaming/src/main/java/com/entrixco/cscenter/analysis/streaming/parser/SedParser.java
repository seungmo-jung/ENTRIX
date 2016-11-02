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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import scala.Tuple2;

public class SedParser extends StreamParser{
	
	private static final Logger logger = LoggerFactory.getLogger(SedParser.class);
	
	public static void parse(final HashMap<String, Object> configMap,JavaStreamingContext jstCtx) {
		
		String topics = (String)configMap.get("sedTopic");
		String brokers = (String)configMap.get("brokers");
		String zookeeper = (String)configMap.get("zookeeper");
		String groupid = (String)configMap.get("sparkGroupID");
		int numThreads = Integer.parseInt((String)configMap.get("numThreads"));
		
//		JavaPairInputDStream<String, String> pairstream = createKafkaDStream(topics,brokers,jstCtx);
//		JavaPairInputDStream<String, String> pairstream = createKafkaDStream2(topics,brokers,groupid,jstCtx,zookeeper);
		JavaPairInputDStream<String, String> pairstream = createKafkaStream(jstCtx,zookeeper,groupid,topics,numThreads);
		
		JavaDStream<Row> linestream = pairstream.map(new Function<Tuple2<String, String>, Row>() {
			public Row call(Tuple2<String, String> t2) { 
//				JsonObject json = new JsonParser().parse(t2._2()).getAsJsonObject();
//				String line = json.get("message").getAsString();
				String line = t2._2;
				String ar[] = line.split(",");
				for(int i=0;i<ar.length;i++){
					ar[i] = ar[i].trim();
				}
				String dt = ar[1].substring(0,10).replace("-","");
				String hh = ar[1].substring(11,13);
				String sar = Arrays.toString(ar).replace(", ", ",");
				String[] ra = ("csrid"+","+sar.substring(1, sar.length()-1)+","+dt+","+hh).split(",");
//				logger.info(">>>>> vals = {}",Arrays.toString(ra).replace(", ", ","));
				
				return RowFactory.create(ra);
			}
		});
		
		String tablename = configMap.get("sedTable").toString();
		String[] columns = configMap.get("sedColumns").toString().split(",");
		HiveContext hiveCtx = (HiveContext)configMap.get("hiveCtx");
		
		saveTable(linestream,tablename,columns,hiveCtx);
	}
}

package com.entrixco.cscenter.analysis.streaming.parser;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.ivy.plugins.namespace.Namespace;
import org.apache.spark.api.java.JavaPairRDD;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import scala.Tuple2;

public class ApiParser extends StreamParser{

	private static final Logger logger = LoggerFactory.getLogger(ApiParser.class);
	
	public static void parse(final HashMap<String, Object> configMap,JavaStreamingContext jstCtx) {
		
		String topics = (String)configMap.get("apiTopic");
		String brokers = (String)configMap.get("brokers");
		String zookeeper = (String)configMap.get("zookeeper");
		String zoohost = (String)configMap.get("zoohost");
		String groupid = (String)configMap.get("sparkGroupID");
		int numThreads = Integer.parseInt((String)configMap.get("numThreads"));
		
//		JavaPairInputDStream<String, String> pairstream = createKafkaDStream(topics,brokers,jstCtx);
		JavaPairInputDStream<String, String> pairstream = createKafkaDStream(topics,brokers,groupid,jstCtx);
//		JavaPairInputDStream<String, String> pairstream = createKafkaStream(jstCtx,zookeeper,groupid,topics,numThreads);
		
		JavaDStream<Row> linestream = pairstream.map(new Function<Tuple2<String, String>, Row>() {
			public Row call(Tuple2<String, String> t2) {
				String[] rt = null;
				try {
//					JsonObject json = new JsonParser().parse(t2._2()).getAsJsonObject();
//					String line = json.get("message").getAsString();
					String line = t2._2;
					String[] ar = line.split(",");
					for(int i=0;i<ar.length;i++){
						ar[i]=ar[i].split("=")[1].trim();
					}
					//stb_id {} 제거
					ar[7]=subData(ar[7],"{");
					String dt = ar[0].substring(0,10).replace("-","");
					String hh = ar[0].substring(11,13);
					rt = new String[ar.length+3];
					rt[0] = "csr_id";
					System.arraycopy(ar, 0, rt, 1, ar.length);
					rt[ar.length+1] = dt;
					rt[ar.length+2] = hh;
					
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				return RowFactory.create(rt);
			}
		});
		
		String tablename = configMap.get("apiTable").toString();
		String[] columns = configMap.get("apiColumns").toString().split(",");
		HiveContext hiveCtx = (HiveContext)configMap.get("hiveCtx");
		
		saveTable(linestream,tablename,columns,hiveCtx);

	}

}

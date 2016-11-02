package com.entrixco.cscenter.analysis.streaming.parser;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import scala.Serializable;

public abstract class StreamParser implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(StreamParser.class);
	
	//public abstract void parse(final HashMap<String, Object> configMap,JavaStreamingContext jstCtx);
	
	public static JavaPairInputDStream<String, String> createKafkaDStream(String topics,String brokers,JavaStreamingContext jstCtx){
		
		HashSet<String> topicSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		HashMap<String, String> kafkaParams= new HashMap<String, String>();
//		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("bootstrap.servers", brokers);
		
		logger.info("topics = "+topics);
		logger.info("brokers = "+brokers);
		
		JavaPairInputDStream<String, String> pairstream = KafkaUtils.createDirectStream(
				jstCtx, String.class, String.class, StringDecoder.class, StringDecoder.class,
				kafkaParams, topicSet);
		
		return pairstream;
	}
	public static JavaPairInputDStream<String, String> createKafkaDStream(String topics,String brokers,String gid,JavaStreamingContext jstCtx){
		
		HashSet<String> topicSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		HashMap<String, String> kafkaParams= new HashMap<String, String>();
//		kafkaParams.put("metadata.broker.list", brokers);
//		kafkaParams.put("zookeeper.connect", zookeeper);
		kafkaParams.put("bootstrap.servers", brokers);
		kafkaParams.put("group.id", gid);
		kafkaParams.put("offsets.storage", "kafka");
		kafkaParams.put("dual.commit.enabled", "false");
//		kafkaParams.put("auto.offset.reset", "smallest");
		
		logger.info("topics = "+topics);
		logger.info("brokers = "+brokers);
		
		JavaPairInputDStream<String, String> pairstream = KafkaUtils.createDirectStream(
				jstCtx, String.class, String.class, StringDecoder.class, StringDecoder.class,
				kafkaParams, topicSet);
		
		return pairstream;
	}
	public static JavaDStream<String> createKafkaDStream(String topics,String brokers,String gid,JavaStreamingContext jstCtx,CuratorFramework  curatorFramework){
		HashMap<String, String> kafkaParams= new HashMap<String, String>();
//		kafkaParams.put("bootstrap.servers", brokers);
//		kafkaParams.put("group.id", gid);
//		kafkaParams.put("auto.offset.reset", "smallest");
		
		int par = 1;
		Map<TopicAndPartition,Long> fromOffsets = new HashMap<TopicAndPartition,Long>();
		for(String topic : topics.split(",")){
			kafkaParams.put("bootstrap.servers", brokers);
			kafkaParams.put("topic."+topic, topic);
			kafkaParams.put("group."+topic, gid);
			kafkaParams.put("offsets.storage", "kafka");
//			kafkaParams.put("auto.offset.reset", "latest");
			
			for(int i=0;i<par;i++){
				try {
					String nodePath = "/"+gid+"/offsets/"+topic+"/"+i;
					byte[] offset = "0".getBytes();
					if(curatorFramework.checkExists().forPath(nodePath)!=null){
						offset = curatorFramework.getData().forPath(nodePath);						
					}
					fromOffsets.put(new TopicAndPartition(topic,i), Long.parseLong(new String(offset)));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		JavaDStream<String> prstream = KafkaUtils.createDirectStream(
					jstCtx, String.class, String.class, StringDecoder.class, StringDecoder.class,String.class,kafkaParams, fromOffsets,
					new Function <MessageAndMetadata<String,String>, String>(){
						public String call(MessageAndMetadata<String,String> me){return me.message();}
					});
		
		return prstream;
	}
	
	public static JavaDStream<String> rsDS(JavaStreamingContext jstCtx, HashMap<String, String> kafkaParams, Map<TopicAndPartition,Long> fromOffsets){
		JavaDStream<String> prstream = null;
		try {
			Iterator<TopicAndPartition> keys = fromOffsets.keySet().iterator();
			while(keys.hasNext()){
				TopicAndPartition tap = keys.next();
				Long off = fromOffsets.get(tap) - 100;
				fromOffsets.put(tap, off);
			}
			prstream = KafkaUtils.createDirectStream(
					jstCtx, String.class, String.class, StringDecoder.class, StringDecoder.class,String.class,kafkaParams, fromOffsets,
					new Function <MessageAndMetadata<String,String>, String>(){
						public String call(MessageAndMetadata<String,String> me){return me.message();}
					});
		} catch (IllegalArgumentException e) {
			prstream = rsDS(jstCtx, kafkaParams, fromOffsets);
		}
		return prstream;
	}
	
	public static JavaPairInputDStream<String, String> createKafkaStream(JavaStreamingContext jstCtx,String zk,String gid,String topics,int numThreads){
		logger.info(">>>>>>>>>>>>>>>>>>zookeeper : "+zk);
		logger.info(">>>>>>>>>>>>>>>>>>group : "+gid);
		logger.info(">>>>>>>>>>>>>>>>>>topics : "+topics);
		logger.info(">>>>>>>>>>>>>>>>>>numThreads : "+numThreads);
		
		Map<String, Integer> topicMap = new HashMap<>();
		String[] topic = topics.split(",");
		for(String top : topic){
			topicMap.put(top, numThreads);
		}
		
		JavaPairInputDStream<String, String> prstream = KafkaUtils.createStream(jstCtx, zk, gid, topicMap);
		
		return prstream;
	}
	
	public static void saveTable(JavaDStream<Row> linestream,final String tablename,final String[] columns,final HiveContext hiveCtx){
		linestream.foreachRDD(new Function<JavaRDD<Row>, Void>() {
			public Void call(JavaRDD<Row> jrdd) {
					
				StructField[] fields = new StructField[columns.length];
				for(int f=0; f<fields.length; f++) {
					fields[f] = DataTypes.createStructField(columns[f], DataTypes.StringType, true);
				}
				StructType struct = new StructType(fields);
				DataFrame df = hiveCtx.createDataFrame(jrdd, struct);
				
				String[] partition ={"dt","hh"};
				logger.info("Save Table = "+tablename);
				df.write().mode(SaveMode.Append).partitionBy(partition).saveAsTable(tablename);
//				df.write().format("orc").mode(SaveMode.Append).partitionBy(partition).saveAsTable(tablename);
//				df.write().format("parquet").mode(SaveMode.Append).partitionBy(partition).saveAsTable(tablename);
				
				return null;
			}
		});
	}

	public static void saveTable(JavaDStream<Row> linestream,final String tablename,final String[] columns,final HiveContext hiveCtx,final String gid,final CuratorFramework  curatorFramework,final AtomicReference<OffsetRange[]> offsetRanges){
		linestream.foreachRDD(new Function<JavaRDD<Row>, Void>() {
			public Void call(JavaRDD<Row> jrdd) throws Exception {
				try {

					StructField[] fields = new StructField[columns.length];
					for(int f=0; f<fields.length; f++) {
						fields[f] = DataTypes.createStructField(columns[f], DataTypes.StringType, true);
					}
					StructType struct = new StructType(fields);
					DataFrame df = hiveCtx.createDataFrame(jrdd, struct);

					String[] partition ={"dt","hh"};
					logger.info("Save Table = "+tablename);
					df.write().mode(SaveMode.Append).partitionBy(partition).saveAsTable(tablename);
//					df.write().format("orc").mode(SaveMode.Append).partitionBy(partition).saveAsTable(tablename);
//					df.write().format("parquet").mode(SaveMode.Append).partitionBy(partition).saveAsTable(tablename);

					final ObjectMapper objectMapper = new ObjectMapper();
					for (OffsetRange o : offsetRanges.get()) {
						 final byte[] offsetBytes = objectMapper.writeValueAsBytes(o.untilOffset());
		                String nodePath = "/"+gid+"/offsets/"+o.topic()+"/"+o.partition();
		                if(curatorFramework.checkExists().forPath(nodePath)!=null){
		                    curatorFramework.setData().forPath(nodePath,offsetBytes);
		                }else{
		                    curatorFramework.create().creatingParentsIfNeeded().forPath(nodePath, offsetBytes);
		                }
		                logger.info(">>>>>>> topic : {}",o.topic());
		                logger.info(">>>>>>> partition : {}",o.partition());
		                logger.info(">>>>>>> from : {}",o.fromOffset());
		                logger.info(">>>>>>> untill : {}",o.untilOffset());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				return null;
			}
		});
	}

	public static String subData(String data, String start){
		String rs;
		if(data.startsWith(start)){
			rs = data.substring(1, data.length()-1);
		}else{
			rs = data;
		}
		return rs;
	}

	public static String setTime(long longTime){
		String time="";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		
		Date ld = new Date(longTime*1000);
		
		time = sdf.format(ld);
		
		return time;
	}
	public static long btl(byte[] by) {
		return Long.parseLong(new String(by));
	}
}

package com.entrixco.cscenter.analysis.streaming.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

public class KafkaGroup {

	private static final Logger logger = LoggerFactory.getLogger(KafkaGroup.class);
	
	public static void main(String[] args) {
		HashMap<String, String> argMap = StreamConfig.readArgs(args);
		String configPath = StreamConfig.CONFIG_PATH;
		if(argMap.containsKey("csconfig")) configPath = argMap.get("csconfig");
		HashMap<String, Object> configMap = StreamConfig.readConfig(configPath);
		logger.info("ConfigPath : "+configPath);
		
		SparkConf spCnf = new SparkConf().setAppName("KafkaGroup");
		SparkContext spCtx = new SparkContext(spCnf);
		JavaSparkContext jspCtx = new JavaSparkContext(spCtx);
		JavaStreamingContext jstCtx = new JavaStreamingContext(jspCtx, Durations.seconds(5));
		
		String topics = (String)configMap.get("GroupTopic");
		final String gid = (String)configMap.get("GroupID");
		String zoohost = (String)configMap.get("zoohost");
		
		String brokers = (String)configMap.get("brokers");
		HashMap<String, String> kafkaParams= new HashMap<String, String>();
		kafkaParams.put("bootstrap.servers", brokers);
		kafkaParams.put("group.id", gid);
		
		logger.info("topics = "+topics);
		logger.info("brokers = "+brokers);
		final CuratorFramework  curatorFramework = CuratorFrameworkFactory.builder().namespace("consumers").connectString(zoohost).connectionTimeoutMs(1000).sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
	    curatorFramework.start();
	    
		int par = 1;
		Map<TopicAndPartition,Long> fromOffsets = new HashMap<TopicAndPartition,Long>();
		for(String topic : topics.split(",")){
			for(int i=0;i<par;i++){
				try {
					String nodePath = "/"+gid+"/offsets/"+topic+"/"+i;
					byte[] offset = curatorFramework.getData().forPath(nodePath);
					fromOffsets.put(new TopicAndPartition(topic,i), btl(offset));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		JavaDStream<String> prstream = KafkaUtils.createDirectStream(
				jstCtx, String.class, String.class, StringDecoder.class, StringDecoder.class,String.class,kafkaParams, fromOffsets,
				new Function <MessageAndMetadata<String,String>, String>(){
					public String call(MessageAndMetadata<String,String> me){
						return me.message();
					}
				});
		prstream.print();
		
		
		jstCtx.start();
		jstCtx.awaitTermination();
	}
	public static long btl(byte[] by) {
		long l = Long.parseLong(new String(by));
		logger.info(">>>>>i {}",l);
		return l;
	}
	
}



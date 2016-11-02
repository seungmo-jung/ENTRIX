package com.entrixco.cscenter.analysis.streaming;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.streaming.parser.CollectdParser2;
import com.entrixco.cscenter.analysis.streaming.parser.CollectdWinParser;
import com.entrixco.cscenter.analysis.streaming.parser.NetworkParser2;
import com.entrixco.cscenter.analysis.streaming.util.StreamConfig;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MetricStream {
	
	private static final Logger logger = LoggerFactory.getLogger(MetricStream.class);
	
	public static void main(String[] args) {
		try {
			HashMap<String, String> argMap = StreamConfig.readArgs(args);
			String configPath = StreamConfig.CONFIG_PATH;
			if(argMap.containsKey("csconfig")) configPath = argMap.get("csconfig");
			HashMap<String, Object> configMap = StreamConfig.readConfig(configPath);
			logger.info("ConfigPath : "+configPath);
			
			SparkConf spCnf = new SparkConf().setAppName("MetricStream");
			
			String driver = (String)configMap.get("metricstream.spark.driver.port");
			String block = (String)configMap.get("metricstream.spark.blockManager.port");
			String fileserver = (String)configMap.get("metricstream.spark.fileserver.port");
			String broadcast = (String)configMap.get("metricstream.spark.broadcast.port");
			if(driver!=null) spCnf.set("spark.driver.port", driver);
			if(block!=null) spCnf.set("spark.blockManager.port", block);
			if(fileserver!=null) spCnf.set("spark.fileserver.port", fileserver);
			if(broadcast!=null) spCnf.set("spark.broadcast.port", broadcast);
			
			spCnf.set("spark.executor.instances", "3");
			
			SparkContext spCtx = new SparkContext(spCnf);
			JavaSparkContext jspCtx = new JavaSparkContext(spCtx);
			HiveContext hiveCtx = new HiveContext(spCtx);
			hiveCtx.sql("set hive.exec.dynamic.partition=true");
		    hiveCtx.sql("set hive.exec.dynamic.partition.mode=nonstrict");
		    configMap.put("hiveCtx", hiveCtx);
			JavaStreamingContext jstCtx = new JavaStreamingContext(jspCtx, Durations.seconds(10));
			jstCtx.addStreamingListener(new StreamingListener() {
				public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
					logger.info("####################################################################");
					logger.warn(">>>>>>>>>>>>>>>>> batch completed : records={}, submission={}, schedule={}, process={}"
							, batchCompleted.batchInfo().numRecords()
							, new Date(batchCompleted.batchInfo().submissionTime()).toString()
							, batchCompleted.batchInfo().schedulingDelay().toString()
							, batchCompleted.batchInfo().processingDelay().toString());
							
					logger.info("####################################################################");
				}
				public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {}
				public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {}
				public void onReceiverError(StreamingListenerReceiverError receiverError) {}
				public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {}
				public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {}
			});
			
			String zoohost = (String)configMap.get("zoohost");
		    final CuratorFramework  curatorFramework = CuratorFrameworkFactory.builder().namespace("kafka/yarn/koya/consumers").connectString(zoohost).connectionTimeoutMs(1000).sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
		    curatorFramework.start();
		    configMap.put("curatorFramework", curatorFramework);
			
			CollectdParser2.parse(configMap, jstCtx);
			CollectdWinParser.parse(configMap, jstCtx);
			NetworkParser2.parse(configMap, jstCtx);
			
			jstCtx.start();
			logger.info("<<<<<<<<<<<<<<<<<<<<< jsContext started >>>>>>>>>>>>>>>>>>>>>");
			jstCtx.awaitTermination();
			
			List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		    JavaRDD<Integer> distData = jspCtx.parallelize(data);
		    distData.collect();
		    
			logger.info("<<<<<<<<<<<<<<<<<<<<<< jsContext terminated >>>>>>>>>>>>>>>>>>>>>");
		} catch (IllegalArgumentException e) {
			
			logger.info(">>>>>>>>>>>>>>>>>>>>>>zookeeper offset reset");
			
			HashMap<String, String> argMap = StreamConfig.readArgs(args);
			String configPath = StreamConfig.CONFIG_PATH;
			if(argMap.containsKey("csconfig")) configPath = argMap.get("csconfig");
			HashMap<String, Object> configMap = StreamConfig.readConfig(configPath);
			logger.info("ConfigPath : "+configPath);
			
			String zoohost = (String)configMap.get("zoohost");
		    final CuratorFramework  curatorFramework = CuratorFrameworkFactory.builder().namespace("kafka/yarn/koya/consumers").connectString(zoohost).connectionTimeoutMs(1000).sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
		    curatorFramework.start();
		    configMap.put("curatorFramework", curatorFramework);
		    try {
			    
			    final ObjectMapper objectMapper = new ObjectMapper();
				final byte[] offsetBytes = objectMapper.writeValueAsBytes((long)0);
				curatorFramework.setData().forPath("/consumer-spark/offsets/resource-collectd/0",offsetBytes);
				curatorFramework.setData().forPath("/consumer-spark/offsets/resource-collectdwin/0",offsetBytes);
				curatorFramework.setData().forPath("/consumer-spark/offsets/resource-snmp/0",offsetBytes);
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		    
			SparkConf spCnf = new SparkConf().setAppName("MetricStream2");
			
			
			spCnf.set("spark.executor.instances", "3");
			
			SparkContext spCtx = new SparkContext(spCnf);
			JavaSparkContext jspCtx = new JavaSparkContext(spCtx);
			HiveContext hiveCtx = new HiveContext(spCtx);
			hiveCtx.sql("set hive.exec.dynamic.partition=true");
		    hiveCtx.sql("set hive.exec.dynamic.partition.mode=nonstrict");
		    configMap.put("hiveCtx", hiveCtx);
			JavaStreamingContext jstCtx = new JavaStreamingContext(jspCtx, Durations.seconds(10));
			jstCtx.addStreamingListener(new StreamingListener() {
				public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
					logger.info("####################################################################");
					logger.info(">>>>>>>>>>>>>>>>> batch completed : records={}, submission={}, schedule={}, process={}"
							, batchCompleted.batchInfo().numRecords()
							, new Date(batchCompleted.batchInfo().submissionTime()).toString()
							, batchCompleted.batchInfo().schedulingDelay().toString()
							, batchCompleted.batchInfo().processingDelay().toString());
							
					logger.info("####################################################################");
				}
				public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {}
				public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {}
				public void onReceiverError(StreamingListenerReceiverError receiverError) {}
				public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {}
				public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {}
			});
			
			CollectdParser2.parse(configMap, jstCtx);
			CollectdWinParser.parse(configMap, jstCtx);
			NetworkParser2.parse(configMap, jstCtx);
			
			jstCtx.start();
			logger.info("<<<<<<<<<<<<<<<<<<<<< jsContext started >>>>>>>>>>>>>>>>>>>>>");
			jstCtx.awaitTermination();
			logger.info("<<<<<<<<<<<<<<<<<<<<<< jsContext terminated >>>>>>>>>>>>>>>>>>>>>");
		}
		
		
	}
	
}




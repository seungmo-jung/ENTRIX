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
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.streaming.parser.CsrParser2;
import com.entrixco.cscenter.analysis.streaming.parser.CsrWebParser;
import com.entrixco.cscenter.analysis.streaming.parser.SedParser2;
import com.entrixco.cscenter.analysis.streaming.parser.WebParser;
import com.entrixco.cscenter.analysis.streaming.parser.ApiParser2;
import com.entrixco.cscenter.analysis.streaming.util.StreamConfig;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Serializable;

public class LogStream implements Serializable {
	
	private static final Logger logger = LoggerFactory.getLogger(LogStream.class);
	
	
	public static void main(String[] args) {
		try {
			HashMap<String, String> argMap = StreamConfig.readArgs(args);
			String configPath = StreamConfig.CONFIG_PATH;
			if(argMap.containsKey("csconfig")) configPath = argMap.get("csconfig");
			final HashMap<String, Object> configMap = StreamConfig.readConfig(configPath);
			logger.info("ConfigPath : "+configPath);
			
			String zoohost = (String)configMap.get("zoohost");
		    final CuratorFramework  curatorFramework = CuratorFrameworkFactory.builder().namespace("kafka/yarn/koya/consumers").connectString(zoohost).connectionTimeoutMs(1000).sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
		    curatorFramework.start();
		    configMap.put("curatorFramework", curatorFramework);
		    
		    //Context생성
		    SparkConf spCnf = new SparkConf().setAppName("LogStream");
		    
		    String driver = (String)configMap.get("logstream.spark.driver.port");
		    String block = (String)configMap.get("logstream.spark.blockManager.port");
		    String fileserver = (String)configMap.get("logstream.spark.fileserver.port");
		    String broadcast = (String)configMap.get("logstream.spark.broadcast.port");
		    if(driver!=null) spCnf.set("spark.driver.port", driver);
		    if(block!=null) spCnf.set("spark.blockManager.port", block);
		    if(fileserver!=null) spCnf.set("spark.fileserver.port", fileserver);
		    if(broadcast!=null) spCnf.set("spark.broadcast.port", broadcast);
		    
//		    spCnf.set("spark.executor.instances", "3");
//		    spCnf.set("spark.app.id", "123");
		    
		    SparkContext spCtx = new SparkContext(spCnf);
		    final JavaSparkContext jspCtx = new JavaSparkContext(spCtx);
		    final HiveContext hiveCtx = new HiveContext(spCtx);
		    
		    
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
		    
//		    hiveCtx.implicits();
		    hiveCtx.sql("set hive.exec.dynamic.partition=true");
		    hiveCtx.sql("set hive.exec.dynamic.partition.mode=nonstrict");
	//	    hiveCtx.sql("set hive.exec.max.dynamic.partitions=100000");
	//	    hiveCtx.sql("set hive.exec.max.dynamic.partitions.pernode=10000");
		    
		    configMap.put("hiveCtx", hiveCtx);
		    
		    
		    
		    CsrParser2.parse(configMap,jstCtx);
		    ApiParser2.parse(configMap,jstCtx);
		    SedParser2.parse(configMap,jstCtx);
		    WebParser.parse(configMap, jstCtx);
		    CsrWebParser.parse(configMap, jstCtx);
			
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
			final HashMap<String, Object> configMap = StreamConfig.readConfig(configPath);
			logger.info("ConfigPath : "+configPath);
			
			String zoohost = (String)configMap.get("zoohost");
		    final CuratorFramework  curatorFramework = CuratorFrameworkFactory.builder().namespace("kafka/yarn/koya/consumers").connectString(zoohost).connectionTimeoutMs(1000).sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
		    curatorFramework.start();
		    configMap.put("curatorFramework", curatorFramework);
		    try {
			    
			    final ObjectMapper objectMapper = new ObjectMapper();
				final byte[] offsetBytes = objectMapper.writeValueAsBytes((long)0);
				curatorFramework.setData().forPath("/consumer-spark/offsets/log-csr/0",offsetBytes);
				curatorFramework.setData().forPath("/consumer-spark/offsets/log-api/0",offsetBytes);
				curatorFramework.setData().forPath("/consumer-spark/offsets/log-csr-web/0",offsetBytes);
				curatorFramework.setData().forPath("/consumer-spark/offsets/log-web/0",offsetBytes);
				curatorFramework.setData().forPath("/consumer-spark/offsets/log-sed/0",offsetBytes);
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		    //Context생성
		    SparkConf spCnf = new SparkConf().setAppName("LogStream2");
		    
		    SparkContext spCtx = new SparkContext(spCnf);
		    final JavaSparkContext jspCtx = new JavaSparkContext(spCtx);
		    final HiveContext hiveCtx = new HiveContext(spCtx);
		    
		    
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
		    
		    hiveCtx.implicits();
		    hiveCtx.sql("set hive.exec.dynamic.partition=true");
		    hiveCtx.sql("set hive.exec.dynamic.partition.mode=nonstrict");
		    configMap.put("hiveCtx", hiveCtx);
		    CsrParser2.parse(configMap,jstCtx);
		    ApiParser2.parse(configMap,jstCtx);
		    SedParser2.parse(configMap,jstCtx);
		    WebParser.parse(configMap, jstCtx);
		    CsrWebParser.parse(configMap, jstCtx);
			
			jstCtx.start();
			logger.info("<<<<<<<<<<<<<<<<<<<<< jsContext started >>>>>>>>>>>>>>>>>>>>>");
			jstCtx.awaitTermination();
			logger.info("<<<<<<<<<<<<<<<<<<<<<< jsContext terminated >>>>>>>>>>>>>>>>>>>>>");
			
			
		}
	}
	
}
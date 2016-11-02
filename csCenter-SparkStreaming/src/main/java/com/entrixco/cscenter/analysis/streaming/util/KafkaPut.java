package com.entrixco.cscenter.analysis.streaming.util;

import java.io.BufferedReader;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.CronScheduleBuilder.cronSchedule;

public class KafkaPut {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaPut.class);

	private static HashMap<String, Object> configMap;
	private static Producer<String, String> producer;

	public static void main(String[] args) {
		//configMap 생성
		HashMap<String, String> argMap = StreamConfig.readArgs(args);
		String configPath = StreamConfig.CONFIG_PATH;
		if(argMap.containsKey("csconfig")) configPath = argMap.get("csconfig");
		logger.info(">>>>>>>>>>>>>>KafkaPut start");
		logger.info("set configPath : "+configPath);
		configMap = StreamConfig.readConfig(configPath);
		
		//producer 생성
		Properties props = new Properties();
		props.put("metadata.broker.list", configMap.get("brokers").toString());
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		producer = new Producer<String, String>(new ProducerConfig(props));
		
		try{
			String filepath = configMap.get("putFilepath").toString();
			if(argMap.containsKey("putFile")) filepath = argMap.get("putFile");
			File file = new File(filepath);
			logger.info("FilePath = "+file.getPath());
			BufferedReader br = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8);
			List<KeyedMessage<String, String>> messages = new ArrayList<KeyedMessage<String, String>>();
			
			String csr_id = file.getName().substring(0,3);
			
			String line;
			String puttopic = configMap.get("putTopic").toString();
			if(argMap.containsKey("putTopic")) puttopic = argMap.get("putTopic");
			logger.info("puttopic = "+puttopic);
			long startTime = System.currentTimeMillis();
			int cnt=0;
			while ((line = br.readLine()) != null) {
				messages.add(new KeyedMessage<String, String>(puttopic,csr_id,line));
				cnt++;
				if((cnt%100)==0){
					producer.send(messages);
//					logger.info("Send Messages cnt : "+ cnt);
					messages.clear();
				}
//				logger.info("Send Messages : "+ line);
			}
			if(cnt%100!=0){
				producer.send(messages);
			}
			logger.info("Send Messages cnt : "+ cnt);
			br.close();
			
			long endTime = System.currentTimeMillis();
		    long elapsedTime = endTime - startTime;
		    logger.info(">>>>>> Execute Time : "+elapsedTime/1000.0);
		    
		}catch(Exception e){
			e.printStackTrace();
		}

		
//		try {
//			Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
//			
//			scheduler.start();
//			
//			//job1설정
//			JobDetail job1 = newJob(Job1.class).withIdentity("job1", "group1").build();
//			Trigger trigger1 = newTrigger().withIdentity("trigger1", "group1").withSchedule(cronSchedule(configMap.get("job1cron").toString())).build();
//			logger.info("job1corn : "+configMap.get("job1cron").toString());
//			
//			//job2설정
//			JobDetail job2 = newJob(Job2.class).withIdentity("job2", "group1").build();
//			Trigger trigger2 = newTrigger().withIdentity("trigger2", "group1").withSchedule(cronSchedule(configMap.get("job2cron").toString())).build();
//			logger.info("job2corn : "+configMap.get("job2cron").toString());
//			
//			//job실행
//			scheduler.scheduleJob(job1,trigger1);
//			scheduler.scheduleJob(job2,trigger2);
//			
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
	}
	
	
	public static HashMap<String, Object> getConfigMap() {
		return configMap;
	}

	public static Producer<String, String> getProducer() {
		return producer;
	}

}


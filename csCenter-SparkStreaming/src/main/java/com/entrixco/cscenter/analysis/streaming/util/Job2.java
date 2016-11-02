package com.entrixco.cscenter.analysis.streaming.util;

import java.io.BufferedReader;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

public class Job2 implements Job{
	
	private static final Logger logger = LoggerFactory.getLogger(Job2.class);
	
	@Override
	public void execute (JobExecutionContext jobExecutionContext) throws JobExecutionException {
		HashMap<String, Object> configMap = KafkaPut.getConfigMap();
		Producer<String, String> producer = KafkaPut.getProducer();
		
		try {
			File file = new File(configMap.get("job2filepath").toString());
			logger.info("FilePath = "+file.getPath());
			BufferedReader br = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8);
			List<KeyedMessage<String, String>> messages = new ArrayList<KeyedMessage<String, String>>();
			
			String csr_id = file.getName().substring(0,3);
			
			String line;
			String puttopic = configMap.get("job2topic").toString();
			while ((line = br.readLine()) != null) {
				producer.send(new KeyedMessage<String, String>(puttopic,csr_id,line));
            	logger.info("Send Messages : topic = "+puttopic+" Message = "+line);
            }

            br.close();
            //producer.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}

package com.entrixco.cscenter.analysis.batch.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchConfig {
	
	private static final Logger logger = LoggerFactory.getLogger(BatchConfig.class);
	
	public static final String CONFIG_PATH = "configs/batch_config.properties";
	public static final String SQL_DIR = "sqls/";
	public static final String SCHEDULE_CONFIG_MAP = "scheduleConfigMap";
	
	public static HashMap<String, Object> readConfig(String path) {
		HashMap<String, Object> configMap = new HashMap<String, Object>();
		Properties systems = System.getProperties();
		for(String key : systems.stringPropertyNames())
			configMap.put(key, systems.getProperty(key));
		logger.info("load system properties : size={}", systems.size());
		
		logger.info("loading config path={}", path);
		Properties configs = new Properties();
		InputStream cis = null;
		try {
			File cfile = new File(path);
			if(cfile.exists()) {
				cis = new FileInputStream(cfile);
			}
			else {
				cis = Thread.currentThread()
						.getContextClassLoader()
								.getResourceAsStream(path);
			}
			configs.load(cis);
			for(String key : configs.stringPropertyNames())
				configMap.put(key, configs.getProperty(key));
			logger.info("loaded config : size={}, isfile={}"
					, configs.size(), cfile.exists());
		} catch(Exception e) {
			throw new RuntimeException(e);
		} finally {
			if(cis!=null) {
				try {cis.close();} catch(Exception fe) {}
			}
		}
		return configMap;
	}
	
	public static String getDefault(Map<String, Object> configMap, String job, String arg) {
		String value = (String)configMap.get(job+"."+arg);
		if(value!=null) return value;
		return (String)configMap.get("default."+arg);
	}
	
	public static HashMap<String, String> readArgs(String[] args) {
		HashMap<String, String> argMap = new HashMap<String, String>();
		for(String arg : args) {
			int eqi = arg.indexOf('=');
			if(arg.startsWith("--") && eqi>2) {
				String key = arg.substring(2, eqi);
				String value = arg.substring(eqi+1);
				argMap.put(key, value);
			}
		}
		logger.info("load args : size={}", argMap.size());
		return argMap;
	}

}

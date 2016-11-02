package com.entrixco.cscenter.analysis.batch.mysql;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.batch.util.BatchConfig;

public class JdbcMain {

	private static final Logger logger = LoggerFactory.getLogger(JdbcMain.class);
	
	public static void main(String[] args) {
		HashMap<String, String> argMap = BatchConfig.readArgs(args);
		String configPath = BatchConfig.CONFIG_PATH;
		if(argMap.containsKey("csconfig")) configPath = argMap.get("csconfig");
		HashMap<String, Object> configMap = BatchConfig.readConfig(configPath);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HH");
		
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.HOUR, -2);
		String sd = sdf.format(new Date(cal.getTimeInMillis()));
		Boolean restore = false;
		if(argMap.containsKey("date")){
			sd = argMap.get("date");
			restore = true;
		}
		String[] time = sd.split("-");
		String dt = time[0];
		String hh = time[1];
		configMap.put("dt", dt);
		configMap.put("hh", hh);
		configMap.put("sd", sd);
		
		logger.info(">>>>>>>>>>>>>>>>>dt : {}",dt);
		logger.info(">>>>>>>>>>>>>>>>>hh : {}",hh);
		
		long startTime = System.currentTimeMillis();
		JdbcHour jh = new JdbcHour();
		jh.connectJDBC(configMap);
		
		String[] insert = configMap.get("jdbc.insert").toString().split(",");
		if(argMap.containsKey("insert")) insert = argMap.get("insert").split(",");
		
		int cnt = 0;
		for(String in : insert){
			cnt += jh.insertMysql(in,restore);
		}
		
		logger.info(">>>>>> Total Insert Cnt : {} ({} {})",cnt,dt,hh);
		
		if(hh.equals("00")){
			jh.deleteSpark(dt);
		}
		
		jh.closeJDBC();
		long endTime = System.currentTimeMillis();
	    long elapsedTime = endTime - startTime;
	    logger.info(">>>>>> Execute Time : "+elapsedTime/1000.0);
	}
}

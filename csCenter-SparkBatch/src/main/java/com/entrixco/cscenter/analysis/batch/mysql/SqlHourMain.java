package com.entrixco.cscenter.analysis.batch.mysql;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.batch.util.BatchConfig;
import com.entrixco.cscenter.analysis.batch.util.HiveExecutor;
import com.entrixco.cscenter.analysis.batch.util.SqlMapper;

public class SqlHourMain {
	private static final Logger logger = LoggerFactory.getLogger(SqlHourMain.class);

	public static void main(String[] args) {
		HashMap<String, String> argMap = BatchConfig.readArgs(args);
		String configPath = BatchConfig.CONFIG_PATH;
		if(argMap.containsKey("csconfig")) configPath = argMap.get("csconfig");
		HashMap<String, Object> configMap = BatchConfig.readConfig(configPath);
		
		SparkConf spCnf = new SparkConf().setAppName("SqlHourMain");
		SparkContext spCtx = new SparkContext(spCnf);
		HiveContext hiveCtx = new HiveContext(spCtx);
		
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.HOUR, -1);
		String sd = new SimpleDateFormat("yyyyMMdd-HH").format(new Date(cal.getTimeInMillis()));
		String[] time = sd.split("-");
		if(argMap.containsKey("date")) time = argMap.get("date").split("-");
		
		String dt = time[0];
		String hh = time[1];
		
		logger.info(">>>>>>>>>>>>>>>>>dt : {}",dt);
		logger.info(">>>>>>>>>>>>>>>>>hh : {}",hh);
		
		HiveExecutor hiveExec = new HiveExecutor(SqlMapper.loadSQLMapFromJar());
		hiveExec.setHiveContext(hiveCtx);
		configMap.put("hiveExec", hiveExec);
		
		Properties props = new Properties();
		props.put("user", configMap.get("default.mysql.user"));
		props.put("password", configMap.get("default.mysql.password"));
		props.put("characterEncoding", "utf8");
		configMap.put("props", props);
		
		new StatCcuHour().saveMysql(dt, hh, configMap);
		new StatUvpvHour().saveMysql(dt, hh, configMap);
	}
}

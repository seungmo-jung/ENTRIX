package com.entrixco.cscenter.analysis.batch.util;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveExecutor {
	
	private static final Logger logger = LoggerFactory.getLogger(HiveExecutor.class);
	
	private HashMap<String, String> sqlMap;
	private HiveContext hiveCtx;
	
	public HiveExecutor(HashMap<String, String> sqlMap) {
		//sqlMap = SqlMapper.loadSQLMap();
		this.sqlMap = sqlMap;
	}
	
	public void setHiveContext(HiveContext hiveCtx) {
		this.hiveCtx = hiveCtx;
	}
	
	public String getSQL(String sid) {
		return sqlMap.get(sid);
	}
	
	public DataFrame executeSQL(String sid, Object...params) {
		String sql = sqlMap.get(sid);
		if(params!=null && params.length>0) {
			sql = String.format(sql, params);
		}
		logger.warn("executeSQL {} : {}", sid, sql);
		long startT = System.currentTimeMillis();
		DataFrame df = hiveCtx.sql(sql);
		long finishT = System.currentTimeMillis();
		logger.warn("executeSQL seconds={}", (finishT-startT)/1000.0);
		return df;
	}

}

package com.entrixco.cscenter.analysis.batch.mysql;

import java.util.HashMap;
import java.util.Properties;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.batch.util.HiveExecutor;

public class StatUvpvHour extends MysqlHour{
	
	private static final Logger logger = LoggerFactory.getLogger(StatUvpvHour.class);
	
	private static final String[] STAT_UVPV_SQL = {"mysql@selectSoUvpv","mysql@selectAppUvpv","mysql@selectCssUvpv"};
	private static final String[] STAT_UVPV_TABLE = {"tb_so_uvpv_stats_hour","tb_app_uvpv_stats_hour","tb_css_uvpv_stats_hour"};
	
	public void saveMysql(String dt,String hh, HashMap<String, Object> configMap){
		HiveExecutor hiveExec = (HiveExecutor)configMap.get("hiveExec");
		Properties props = (Properties)configMap.get("props");
		for(int i=0;i<STAT_UVPV_SQL.length;i++){
			logger.info(">>>>>>>>>>>>>>>>>STAT_UVPV_SQL[{}] : {}",i,STAT_UVPV_SQL[i]);
			logger.info(">>>>>>>>>>>>>>>>>STAT_UVPV_TABLE[{}] : {}",i,STAT_UVPV_TABLE[i]);
			DataFrame df = hiveExec.executeSQL(STAT_UVPV_SQL[i],dt,hh);
			if(df.count()>0){
				df.write().mode(SaveMode.Append).jdbc(configMap.get("default.mysql.url").toString(), STAT_UVPV_TABLE[i], props);
			}
		}
	}
}

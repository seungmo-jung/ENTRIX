package com.entrixco.cscenter.analysis.batch;

import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.batch.client.EsClient;
import com.entrixco.cscenter.analysis.batch.client.MysqlClient;
import com.entrixco.cscenter.analysis.batch.util.BatchConfig;
import com.entrixco.cscenter.analysis.batch.util.BatchScheduler;
import com.entrixco.cscenter.analysis.batch.util.HiveExecutor;
import com.entrixco.cscenter.analysis.batch.util.SqlMapper;

public class BatchMain {
	
	private static final Logger logger = LoggerFactory.getLogger(BatchMain.class);
	
	public static void main(String[] args) {
		startApplication(args);
	}
	
	public static void startApplication(String[] args) {
		HashMap<String, String> argMap = BatchConfig.readArgs(args);
		String configPath = BatchConfig.CONFIG_PATH;
		if(argMap.containsKey("csconfig")) configPath = argMap.get("csconfig");
		HashMap<String, Object> configMap = BatchConfig.readConfig(configPath);
		
		//HashMap<String, String> sqlMap = SqlMapper.loadSQLMap();
		HashMap<String, String> sqlMap = SqlMapper.loadSQLMapFromJar();
		configMap.put("sqlMap", sqlMap);
		HiveExecutor hiveExec = new HiveExecutor(sqlMap);
		configMap.put("hiveExec", hiveExec);
		MysqlClient mysqlClient = new MysqlClient(configMap);
		configMap.put("mysqlClient", mysqlClient);
		EsClient esClient = new EsClient(configMap);
		configMap.put("esClient", esClient);
		
		String jobname = argMap.get("csjob");
		configMap.put("csjob", jobname);
		String jobclass = (String)configMap.get("csjob."+jobname);
		String jobgroup = null;
		String jobcron = BatchConfig.getDefault(configMap, jobname, "cron");
		String hivemode = argMap.get("hivemode");
		if(hivemode!=null) configMap.put(jobname+".hive.mode", hivemode);
		String hiveformat = argMap.get("hiveformat");
		if(hiveformat!=null) configMap.put(jobname+".hive.format", hivemode);
		
		if(configMap.get("spark.master")!=null || configMap.get("SPARK_SUBMIT")!=null) {
			SparkConf spCnf = new SparkConf().setAppName(jobname);
			
			String driverPort = BatchConfig.getDefault(configMap, jobname, "spark.driver.port");
			if(driverPort!=null) {
				logger.info("####### spark.driver.port={}", driverPort);
				spCnf.set("spark.driver.port", driverPort); 
			}
			String fileserverPort = BatchConfig.getDefault(configMap, jobname, "spark.fileserver.port");
			if(fileserverPort!=null) {
				logger.info("####### spark.fileserver.port={}", fileserverPort);
				spCnf.set("spark.fileserver.port", fileserverPort); 
			}
			String broadcastPort = BatchConfig.getDefault(configMap, jobname, "spark.broadcast.port");
			if(broadcastPort!=null) {
				logger.info("####### spark.broadcast.port={}", broadcastPort);
				spCnf.set("spark.broadcast.port", broadcastPort); 
			}
			String blockPort = BatchConfig.getDefault(configMap, jobname, "spark.blockManager.port");
			if(blockPort!=null) {
				logger.info("####### spark.blockManager.port={}", blockPort);
				spCnf.set("spark.blockManager.port", blockPort);
			}
			
			String esNodes = BatchConfig.getDefault(configMap, jobname, "es.nodes");
			spCnf.set("es.nodes", esNodes);//"big01:9200,big02:9200");
			spCnf.set("es.index.auto.create", "true");
			
			SparkContext spCtx = new SparkContext(spCnf);
			JavaSparkContext jspCtx = new JavaSparkContext(spCtx);
			SQLContext sqlCtx = new SQLContext(spCtx);
			HiveContext hiveCtx = new HiveContext(spCtx);
			hiveCtx.sql("set hive.exec.dynamic.partition=true");
			hiveCtx.sql("set hive.exec.dynamic.partition.mode=nonstrict");
			int shuffles = BatchConfig.getDefault(configMap, jobname, "shuffle.partitions")==null
					? 0 : Integer.parseInt(BatchConfig.getDefault(configMap, jobname, "shuffle.partitions"));
			if(shuffles>0) {
				hiveCtx.sql("set spark.sql.shuffle.partitions="+shuffles);
				logger.info("################### shuffle partitions={} ################", shuffles);
			}
			configMap.put("spCnf", spCnf);
			configMap.put("spCtx", spCtx);
			configMap.put("jspCtx", jspCtx);
			configMap.put("sqlCtx", sqlCtx);
			configMap.put("hiveCtx", hiveCtx);
			hiveExec.setHiveContext(hiveCtx);
			
			if(argMap.containsKey("window")){
				configMap.put("window", argMap.get("window"));
				configMap.put("mode", argMap.get("mode"));
			}
			
		}
		
		if(argMap.get("once")!=null) {
			BatchScheduler.startOnce(configMap, jobclass, argMap.get("once"));
		}
		else {
			BatchScheduler.startSchedule(configMap, jobclass, jobname, jobgroup, jobcron);
		}
	}

}
/*
			if(jobname.equals("stat_usetime")) {
				//hiveCtx.sql("add jar "+configMap.get("batch.function.path"));
				hiveCtx.sql("create temporary function use10 as 'com.entrixco.cscenter.analysis.batch.udf.Usetime10'");
				hiveCtx.sql("create temporary function use20 as 'com.entrixco.cscenter.analysis.batch.udf.Usetime20'");
				hiveCtx.sql("create temporary function use30 as 'com.entrixco.cscenter.analysis.batch.udf.Usetime30'");
				hiveCtx.sql("create temporary function use40 as 'com.entrixco.cscenter.analysis.batch.udf.Usetime40'");
				hiveCtx.sql("create temporary function use50 as 'com.entrixco.cscenter.analysis.batch.udf.Usetime50'");
				hiveCtx.sql("create temporary function use60 as 'com.entrixco.cscenter.analysis.batch.udf.Usetime60'");
				hiveCtx.sql("create temporary function useother as 'com.entrixco.cscenter.analysis.batch.udf.UsetimeOther'");
			}
 */

//			if(jobname.equals("stat_ccu")) {
//				mysqlClient.open();
//				esClient.open();
//			}
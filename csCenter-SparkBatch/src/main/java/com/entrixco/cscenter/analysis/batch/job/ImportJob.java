package com.entrixco.cscenter.analysis.batch.job;

import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportJob extends BatchJob {
	
private static final Logger logger = LoggerFactory.getLogger(ImportJob.class);
	
	public void execute(JobExecutionContext jobctx) {
		//logger.info("execute : hash={}, thread={}", this.hashCode(), Thread.currentThread().getId());
		
		Map<String, Object> cmap = getConfigMap(jobctx);
		JavaSparkContext jspCtx = (JavaSparkContext)cmap.get("jspCtx");
		HiveContext hiveCtx = (HiveContext)cmap.get("hiveCtx");
		
		String importPath = (String)cmap.get("import.path");
		JavaRDD<String> inrdd = jspCtx.textFile(importPath);
	}

}

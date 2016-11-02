package com.entrixco.cscenter.analysis.batch.job;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.batch.client.EsClient;
import com.entrixco.cscenter.analysis.batch.client.HiveClient;
import com.entrixco.cscenter.analysis.batch.util.BatchConfig;
import com.entrixco.cscenter.analysis.batch.util.HiveExecutor;

public class SedJob extends BatchJob {
	
	private static final Logger logger = LoggerFactory.getLogger(SedJob.class);
	private static final String JOBNAME = "sed";
	
	public void execute(JobExecutionContext jobctx) {
		long beginT = System.currentTimeMillis();
		Map<String, Object> cmap = getConfigMap(jobctx);
		int delay = Integer.parseInt(BatchConfig.getDefault(cmap, JOBNAME, "batch.delay"));
		int window = Integer.parseInt(BatchConfig.getDefault(cmap, JOBNAME, "batch.window"));
		
		////
		if(cmap.containsKey("window")){
			window = Integer.parseInt(cmap.get("window").toString()) * 60;
			delay = 0;
			logger.info(">>>>>>>>>WINDOW: {}",window);
		}
		////
		
		Date curtime = jobctx.getScheduledFireTime();
		Calendar cal = Calendar.getInstance();
		cal.setTime(curtime);
		cal.add(Calendar.SECOND, -delay);
		Date curend = cal.getTime();
		cal.add(Calendar.SECOND, -window);
		Date curbegin = cal.getTime();
		
		String curendmilli = milliForm.format(curend);
		String curenddt = dtForm.format(curend);
		String curendhh = hhForm.format(curend);
		
		String curbeginmilli = milliForm.format(curbegin);
		String curbegindt = dtForm.format(curbegin);
		String curbeginhh = hhForm.format(curbegin);
		
		
		
		////
		String cssinfo_table = "";
		int winMin = window/60;
		long beginStatMin = Long.parseLong(curbeginmilli.substring(0,16).replaceAll("[-: ]", ""));
		if(cmap.containsKey("window")){
			cssinfo_table="cssinfo_tmp";
			updateCssinfoTmp(cmap, winMin, beginStatMin, "CSS", "sed");
		}else{
			cssinfo_table="cssinfo_sed";
			String clp = BatchConfig.getDefault(cmap, JOBNAME, "cssinfo.local.path");
			String chp = BatchConfig.getDefault(cmap, JOBNAME, "cssinfo.hive.path");
			updateCssinfo(cmap, winMin, beginStatMin, clp, chp,"CSS");
		}
		long infoT = System.currentTimeMillis();
		////
		
		long cssT = executeCss(cmap
				, curendmilli, curenddt, curendhh
				, curbeginmilli, curbegindt, curbeginhh, cssinfo_table);
		long soT = executeSo(cmap
				, curendmilli, curenddt, curendhh
				, curbeginmilli, curbegindt, curbeginhh);
		long appT = executeApp(cmap
				, curendmilli, curenddt, curendhh
				, curbeginmilli, curbegindt, curbeginhh);
		
		////
		if(cmap.containsKey("window")){
			HiveContext hiveCtx = (HiveContext)cmap.get("hiveCtx");
			hiveCtx.sql("DROP TABLE cssinfo_tmp");
			logger.info("DROP TABLE cssinfo_tmp");
		}
		////
		
		long endT = System.currentTimeMillis();
		logger.info("##################################################");
		logger.info("info time={}, css time={}, so time={}, app time={}, total time={}",
				(infoT-beginT)/1000.0, (cssT-infoT)/1000.0, (soT-cssT)/1000.0, (appT-soT)/1000.0, (endT-beginT)/1000.0);
		logger.info("##################################################");
		
		//executeDashboard2(cmap, curbegin);
	}
	
	public static long executeCss(Map<String, Object> cmap
			, String curendmilli, String curenddt, String curendhh
			, String curbeginmilli, String curbegindt, String curbeginhh
			, String cssinfo_table
			) {
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame cssdf = hiveExec.executeSQL("sed@selectSedCssM"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbeginmilli, curendmilli, cssinfo_table
				);
		//cssdf.persist();
		long con = System.currentTimeMillis();
		String csstable = BatchConfig.getDefault(cmap, JOBNAME, "css.table");
		String hivemode = BatchConfig.getDefault(cmap, JOBNAME, "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, JOBNAME, "hive.format");
		long hi = System.currentTimeMillis();
		HiveClient.saveTable(csstable, hivemode, hiveformat, cssdf, "dt", "hh");
		long el = System.currentTimeMillis();
		EsClient.saveES(cssdf, csstable+"/docs");
		long end = System.currentTimeMillis();
		
		logger.warn("get conf={}, save hive={}, save es={}",hi-con,el-hi,end-el);
		
		//cssdf.unpersist();
		return System.currentTimeMillis();
	}
	
	public static long executeApp(Map<String, Object> cmap
			, String curendmilli, String curenddt, String curendhh
			, String curbeginmilli, String curbegindt, String curbeginhh) {
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame appdf = hiveExec.executeSQL("sed@selectSedApp"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbeginmilli.substring(0,16).replaceAll("[-: ]", "")
				, curendmilli.substring(0,16).replaceAll("[-: ]", "")
				);
		//appdf.persist();
		long con = System.currentTimeMillis();
		String apptable = BatchConfig.getDefault(cmap, JOBNAME, "app.table");
		String hivemode = BatchConfig.getDefault(cmap, JOBNAME, "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, JOBNAME, "hive.format");
		long hi = System.currentTimeMillis();
		HiveClient.saveTable(apptable, hivemode, hiveformat, appdf, "dt", "hh");
		long el = System.currentTimeMillis();
		EsClient.saveES(appdf, apptable+"/docs");
		long end = System.currentTimeMillis();
		
		logger.warn("get conf={}, save hive={}, save es={}",hi-con,el-hi,end-el);
		
		//appdf.unpersist();
		return System.currentTimeMillis();
	}
	
	public static long executeSo(Map<String, Object> cmap
			, String curendmilli, String curenddt, String curendhh
			, String curbeginmilli, String curbegindt, String curbeginhh) {
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame sodf = hiveExec.executeSQL("sed@selectSedSo"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbeginmilli.substring(0,16).replaceAll("[-: ]", "")
				, curendmilli.substring(0,16).replaceAll("[-: ]", "")
				);
		//sodf.persist();
		long con = System.currentTimeMillis();
		String sotable = BatchConfig.getDefault(cmap, JOBNAME, "so.table");
		String hivemode = BatchConfig.getDefault(cmap, JOBNAME, "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, JOBNAME, "hive.format");
		long hi = System.currentTimeMillis();
		HiveClient.saveTable(sotable, hivemode, hiveformat, sodf, "dt", "hh");
		long el = System.currentTimeMillis();
		EsClient.saveES(sodf, sotable+"/docs");
		long end = System.currentTimeMillis();
		
		logger.warn("get conf={}, save hive={}, save es={}",hi-con,el-hi,end-el);
		
		//sodf.unpersist();
		return System.currentTimeMillis();
	}

}

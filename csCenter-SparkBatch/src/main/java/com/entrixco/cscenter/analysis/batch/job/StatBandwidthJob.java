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

public class StatBandwidthJob extends BatchJob {
	private static final Logger logger = LoggerFactory.getLogger(StatBandwidthJob.class);
	private static final String JOBNAME = "stat_band";
	
	public void execute(JobExecutionContext jobctx) {
		long beginT = System.currentTimeMillis();
		Map<String, Object> cmap = getConfigMap(jobctx);
		int delay = Integer.parseInt(BatchConfig.getDefault(cmap, JOBNAME, "batch.delay"));
		int poswindow = Integer.parseInt(BatchConfig.getDefault(cmap, JOBNAME, "batch.poswindow"));
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
		cal.add(Calendar.SECOND, -poswindow);
		Date curend = cal.getTime();
		cal.add(Calendar.SECOND, -window);
		Date curbegin = cal.getTime();
		
		String curenddate = dateForm.format(curend);
		String curenddt = dtForm.format(curend);
		String curendhh = hhForm.format(curend);
		
		String curbegindate = dateForm.format(curbegin);
		String curbegindt = dtForm.format(curbegin);
		String curbeginhh = hhForm.format(curbegin);
		
		////
		String cssinfo_table = "";
		int winMin = window/60;
		long beginStatMin = Long.parseLong(curbegindate.substring(0,16).replaceAll("[-: ]", ""));
		if(cmap.containsKey("window")){
			cssinfo_table="cssinfo_tmp";
			updateCssinfoTmp(cmap, winMin, beginStatMin, "CSS", "bandwidth");
		}else{
			cssinfo_table="cssinfo_stat_band";
			String clp = BatchConfig.getDefault(cmap, JOBNAME, "cssinfo.local.path");
			String chp = BatchConfig.getDefault(cmap, JOBNAME, "cssinfo.hive.path");
			updateCssinfo(cmap, winMin, beginStatMin, clp, chp,"CSS");
		}
		long infoT = System.currentTimeMillis();
		////
		
		long cssT = executeCss(cmap
				, curenddate, curbegindate
				, curenddt, curbegindt
				, curendhh, curbeginhh, cssinfo_table);
		long soT = executeSo(cmap
				, curenddate, curbegindate
				, curenddt, curbegindt
				, curendhh, curbeginhh, cssinfo_table);
		long appT = executeApp(cmap
				, curenddate, curbegindate
				, curenddt, curbegindt
				, curendhh, curbeginhh, cssinfo_table);
		
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
	}
	
	public static long executeCss(Map<String, Object> cmap
			, String curenddate, String curbegindate
			, String curenddt, String curbegindt
			, String curendhh, String curbeginhh, String cssinfo_table) {
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame cssdf = hiveExec.executeSQL("stat@selectBandwidthCssM"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbegindate, curenddate, cssinfo_table
				);
		//cssdf.persist();
		
		String csstable = BatchConfig.getDefault(cmap, JOBNAME, "css.table");
		String hivemode = BatchConfig.getDefault(cmap, JOBNAME, "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, JOBNAME, "hive.format");
		HiveClient.saveTable(csstable, hivemode, hiveformat, cssdf, "dt", "hh");
		EsClient.saveES(cssdf, csstable+"/docs");
		
		//cssdf.unpersist();
		return System.currentTimeMillis();
	}
	
	public static long executeApp(Map<String, Object> cmap
			, String curenddate, String curbegindate
			, String curenddt, String curbegindt
			, String curendhh, String curbeginhh, String cssinfo_table) {
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame appdf = hiveExec.executeSQL("stat@selectBandwidthAppM"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbegindate, curenddate, cssinfo_table
				);
		//appdf.persist();
		
		String apptable = BatchConfig.getDefault(cmap, JOBNAME, "app.table");
		String hivemode = BatchConfig.getDefault(cmap, JOBNAME, "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, JOBNAME, "hive.format");
		HiveClient.saveTable(apptable, hivemode, hiveformat, appdf, "dt", "hh");
		EsClient.saveES(appdf, apptable+"/docs");
		
		//appdf.unpersist();
		return System.currentTimeMillis();
	}
	
	public static long executeSo(Map<String, Object> cmap
			, String curenddate, String curbegindate
			, String curenddt, String curbegindt
			, String curendhh, String curbeginhh, String cssinfo_table) {
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame sodf = hiveExec.executeSQL("stat@selectBandwidthSoM"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbegindate, curenddate, cssinfo_table
				);
		//sodf.persist();
		
		String sotable = BatchConfig.getDefault(cmap, JOBNAME, "so.table");
		String hivemode = BatchConfig.getDefault(cmap, JOBNAME, "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, JOBNAME, "hive.format");
		HiveClient.saveTable(sotable, hivemode, hiveformat, sodf, "dt", "hh");
		EsClient.saveES(sodf, sotable+"/docs");
		
		//sodf.unpersist();
		return System.currentTimeMillis();
	}

}

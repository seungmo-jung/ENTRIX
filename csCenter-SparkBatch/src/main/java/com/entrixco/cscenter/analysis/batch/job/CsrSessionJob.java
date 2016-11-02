package com.entrixco.cscenter.analysis.batch.job;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.spark.sql.DataFrame;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.batch.client.EsClient;
import com.entrixco.cscenter.analysis.batch.client.HiveClient;
import com.entrixco.cscenter.analysis.batch.util.BatchConfig;
import com.entrixco.cscenter.analysis.batch.util.HiveExecutor;

public class CsrSessionJob extends BatchJob {

	private static final Logger logger = LoggerFactory.getLogger(CsrSessionJob.class);
	private static final String JOBNAME = "csr_session";
	
	public void execute(JobExecutionContext jobctx) {
		long beginT = System.currentTimeMillis();
		Map<String, Object> cmap = getConfigMap(jobctx);
		int delay = Integer.parseInt(BatchConfig.getDefault(cmap, JOBNAME, "batch.delay"));
		int window = Integer.parseInt(BatchConfig.getDefault(cmap, JOBNAME, "batch.window"));
		
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
		int winMin = window/60;
		long beginStatMin = Long.parseLong(curbeginmilli.substring(0,16).replaceAll("[-: ]", ""));
		String clp = BatchConfig.getDefault(cmap, JOBNAME, "cssinfo.local.path");
		String chp = BatchConfig.getDefault(cmap, JOBNAME, "cssinfo.hive.path");
		updateCssinfo(cmap, winMin, beginStatMin, clp, chp,"CSS");
		////
		
		long cssT = executeCss(cmap
				, curendmilli, curenddt, curendhh
				, curbeginmilli, curbegindt, curbeginhh);
		long soT = executeSo(cmap
				, curendmilli, curenddt, curendhh
				, curbeginmilli, curbegindt, curbeginhh);
		long appT = executeApp(cmap
				, curendmilli, curenddt, curendhh
				, curbeginmilli, curbegindt, curbeginhh);
		
		long endT = System.currentTimeMillis();
		logger.info("##################################################");
		logger.info("css time={}, so time={}, app time={}, total time={}",
				(cssT-beginT)/1000.0, (soT-cssT)/1000.0, (appT-soT)/1000.0, (endT-beginT)/1000.0);
		logger.info("##################################################");
	}
	
	public static long executeCss(Map<String, Object> cmap
			, String curendmilli, String curenddt, String curendhh
			, String curbeginmilli, String curbegindt, String curbeginhh
			) {
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame cssdf = hiveExec.executeSQL("csr@selectSessionCssM"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbeginmilli, curendmilli
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
			, String curendmilli, String curenddt, String curendhh
			, String curbeginmilli, String curbegindt, String curbeginhh) {
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame appdf = hiveExec.executeSQL("csr@selectSessionAppM"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbeginmilli.substring(0,16).replaceAll("[-: ]", "")
				, curendmilli.substring(0,16).replaceAll("[-: ]", "")
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
			, String curendmilli, String curenddt, String curendhh
			, String curbeginmilli, String curbegindt, String curbeginhh) {
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame sodf = hiveExec.executeSQL("csr@selectSessionSoM"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbeginmilli.substring(0,16).replaceAll("[-: ]", "")
				, curendmilli.substring(0,16).replaceAll("[-: ]", "")
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

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

public class TpsJob extends BatchJob {
	private static final Logger logger = LoggerFactory.getLogger(TpsJob.class);
	private static final String JOBNAME = "tps";
	
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
		String csrinfo_table = "";
		int winMin = window/60;
		long beginStatMin = Long.parseLong(curbeginmilli.substring(0,16).replaceAll("[-: ]", ""));
		if(cmap.containsKey("window")){
			csrinfo_table="csrinfo_tmp";
			cssinfo_table="cssinfo_tmp";
			updateCsrinfoTmp(cmap, winMin, beginStatMin);
			updateCssinfoTmp(cmap, winMin, beginStatMin, "WEB", "tps");
		}else{
			csrinfo_table="csrinfo_tps";
			cssinfo_table="webinfo_tps";
			String clp = BatchConfig.getDefault(cmap, JOBNAME, "csrinfo.local.path");
			String chp = BatchConfig.getDefault(cmap, JOBNAME, "csrinfo.hive.path");
			String wlp = BatchConfig.getDefault(cmap, JOBNAME, "webinfo.local.path");
			String whp = BatchConfig.getDefault(cmap, JOBNAME, "webinfo.hive.path");
			updateCsrinfo(cmap, winMin, beginStatMin, clp, chp);
			updateCssinfo(cmap, winMin, beginStatMin, wlp, whp,"WEB");
		}
		long infoT = System.currentTimeMillis();
		////
		
		long tpsT = executeTps(cmap
				, curendmilli, curenddt, curendhh
				, curbeginmilli, curbegindt, curbeginhh, cssinfo_table, csrinfo_table);
		long soT = executeSo(cmap
				, curendmilli, curenddt, curendhh
				, curbeginmilli, curbegindt, curbeginhh);
		
		////
		if(cmap.containsKey("window")){
			HiveContext hiveCtx = (HiveContext)cmap.get("hiveCtx");
			hiveCtx.sql("DROP TABLE cssinfo_tmp");
			logger.info("DROP TABLE cssinfo_tmp");
			hiveCtx.sql("DROP TABLE csrinfo_tmp");
			logger.info("DROP TABLE csrinfo_tmp");
		}
		////
		
		long endT = System.currentTimeMillis();
		logger.info("##################################################");
		logger.info("info time={}, tps time={}, so time={}, total time={}",
				(infoT-beginT)/1000.0, (tpsT-infoT)/1000.0, (soT-tpsT)/1000.0, (endT-beginT)/1000.0);
		logger.info("##################################################");
		
	}
	
	public static long executeTps(Map<String, Object> cmap
			, String curendmilli, String curenddt, String curendhh
			, String curbeginmilli, String curbegindt, String curbeginhh
			, String cssinfo_table, String csrinfo_table) {
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame csrdf = hiveExec.executeSQL("tps@selectCsrTps"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbeginmilli, curendmilli, csrinfo_table
				);
		DataFrame webdf = hiveExec.executeSQL("tps@selectWebTps"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbeginmilli, curendmilli, cssinfo_table
				);
		
		String csrtable = BatchConfig.getDefault(cmap, JOBNAME, "csr.table");
		String webtable = BatchConfig.getDefault(cmap, JOBNAME, "web.table");
		String hivemode = BatchConfig.getDefault(cmap, JOBNAME, "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, JOBNAME, "hive.format");
		
		HiveClient.saveTable(csrtable, hivemode, hiveformat, csrdf, "dt", "hh");
		EsClient.saveES(csrdf, csrtable+"/docs");
		HiveClient.saveTable(webtable, hivemode, hiveformat, webdf, "dt", "hh");
		EsClient.saveES(webdf, webtable+"/docs");
		
		return System.currentTimeMillis();
	}
	
	public static long executeSo(Map<String, Object> cmap
			, String curendmilli, String curenddt, String curendhh
			, String curbeginmilli, String curbegindt, String curbeginhh) {
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame socsrdf = hiveExec.executeSQL("tps@selectSoCsrTps"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbeginmilli.substring(0,16).replaceAll("[-: ]", "")
				, curendmilli.substring(0,16).replaceAll("[-: ]", "")
				);
		DataFrame sowebdf = hiveExec.executeSQL("tps@selectSoWebTps"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbeginmilli.substring(0,16).replaceAll("[-: ]", "")
				, curendmilli.substring(0,16).replaceAll("[-: ]", "")
				);
		
		String socsrtable = BatchConfig.getDefault(cmap, JOBNAME, "socsr.table");
		String sowebtable = BatchConfig.getDefault(cmap, JOBNAME, "soweb.table");
		String hivemode = BatchConfig.getDefault(cmap, JOBNAME, "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, JOBNAME, "hive.format");
		HiveClient.saveTable(socsrtable, hivemode, hiveformat, socsrdf, "dt", "hh");
		EsClient.saveES(socsrdf, socsrtable+"/docs");
		HiveClient.saveTable(sowebtable, hivemode, hiveformat, sowebdf, "dt", "hh");
		EsClient.saveES(sowebdf, sowebtable+"/docs");
		
		return System.currentTimeMillis();
	}
}

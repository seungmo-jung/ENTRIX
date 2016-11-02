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

public class CsrStatusJob extends BatchJob {
	
	private static final Logger logger = LoggerFactory.getLogger(CsrStatusJob.class);
	private static final String JOBNAME = "csr_status";
	
	public void execute(JobExecutionContext jobctx) {
		long beginT = System.currentTimeMillis();
		Map<String, Object> cmap = getConfigMap(jobctx);
		int delay = Integer.parseInt(BatchConfig.getDefault(cmap, JOBNAME, "batch.delay"));
		int window = Integer.parseInt(BatchConfig.getDefault(cmap, JOBNAME, "batch.window"));
		int poswindow = Integer.parseInt(BatchConfig.getDefault(cmap, JOBNAME, "batch.poswindow"));
	
		////
		if(cmap.containsKey("window")){
			window = Integer.parseInt(cmap.get("window").toString()) * 60;
			delay=-poswindow;
			logger.info(">>>>>>>>>WINDOW: {}",window);
		}
		////
		
		Date curtime = jobctx.getScheduledFireTime();
		Calendar cal = Calendar.getInstance();
		cal.setTime(curtime);
		cal.add(Calendar.SECOND, -delay);
		Date posend = cal.getTime();
		cal.add(Calendar.SECOND, -poswindow);
		Date curend = cal.getTime();
		cal.add(Calendar.SECOND, -window);
		Date curbegin = cal.getTime();
		
		String posendmilli = milliForm.format(posend);
		String posenddt = dtForm.format(posend);
		String posendhh = hhForm.format(posend);
		
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
			cssinfo_table="cssinfo_tmp";
			csrinfo_table="csrinfo_tmp";
			updateCssinfoTmp(cmap, winMin, beginStatMin, "CSS", "status");
			updateCsrinfoTmp(cmap, winMin, beginStatMin);
		}else{
			cssinfo_table="cssinfo_csr_status";
			csrinfo_table="csrinfo_status";
			String clp = BatchConfig.getDefault(cmap, JOBNAME, "cssinfo.local.path");
			String chp = BatchConfig.getDefault(cmap, JOBNAME, "cssinfo.hive.path");
			String rlp = BatchConfig.getDefault(cmap, JOBNAME, "csrinfo.local.path");
			String rhp = BatchConfig.getDefault(cmap, JOBNAME, "csrinfo.hive.path");
			updateCssinfo(cmap, winMin, beginStatMin, clp, chp,"CSS");
			updateCsrinfo(cmap, winMin, beginStatMin, rlp, rhp);
		}
		long infoT = System.currentTimeMillis();
		////
		
		long cssT = executeCss(cmap
				, posendmilli, posenddt, posendhh
				, curbeginmilli, curbegindt, curbeginhh, curendmilli, cssinfo_table);
		long soT = executeSo(cmap
				, curendmilli, curenddt, curendhh
				, curbeginmilli, curbegindt, curbeginhh);
		long appT = executeApp(cmap
				, curendmilli, curenddt, curendhh
				, curbeginmilli, curbegindt, curbeginhh);
		long csrT = executeCsr(cmap
				, posendmilli, posenddt, posendhh
				, curbeginmilli, curbegindt, curbeginhh, curendmilli, csrinfo_table);

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
		logger.info("info time={}, css time={}, so time={}, app time={}, csrT={}, total time={}",
				(infoT-beginT)/1000.0, (cssT-infoT)/1000.0, (soT-cssT)/1000.0, (appT-soT)/1000.0,
				(appT-csrT)/1000.0, (endT-beginT)/1000.0);
		logger.info("##################################################");
	}
	
	public static long executeCss(Map<String, Object> cmap
			, String posendmilli, String posenddt, String posendhh
			, String curbeginmilli, String curbegindt, String curbeginhh, String curendmilli
			, String cssinfo_table) {
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame cssdf = hiveExec.executeSQL("csr@selectStatusCssM"
				, curbegindt, curbeginhh, posenddt, posendhh
				, curbeginmilli, posendmilli, curendmilli, cssinfo_table
				);
//		DataFrame cssdf = hiveExec.executeSQL("csr@selectStatusCss"
//				, curbegindt, curbeginhh, posenddt, posendhh
//				, curbeginmilli, posendmilli, curendmilli
//				);
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
		DataFrame appdf = hiveExec.executeSQL("csr@selectStatusAppM"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbeginmilli.substring(0,16).replaceAll("[-: ]", "")
				, curendmilli.substring(0,16).replaceAll("[-: ]", "")
				);
//		DataFrame appdf = hiveExec.executeSQL("csr@selectStatusApp"
//				, curbegindt, curbeginhh, curenddt, curendhh
//				, curbeginmilli.substring(0,16).replaceAll("[-: ]", "")
//				, curendmilli.substring(0,16).replaceAll("[-: ]", "")
//				);
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
		DataFrame sodf = hiveExec.executeSQL("csr@selectStatusSoM"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbeginmilli.substring(0,16).replaceAll("[-: ]", "")
				, curendmilli.substring(0,16).replaceAll("[-: ]", "")
				);
//		DataFrame sodf = hiveExec.executeSQL("csr@selectStatusSo"
//				, curbegindt, curbeginhh, curenddt, curendhh
//				, curbeginmilli.substring(0,16).replaceAll("[-: ]", "")
//				, curendmilli.substring(0,16).replaceAll("[-: ]", "")
//				);
		//sodf.persist();
		
		String sotable = BatchConfig.getDefault(cmap, JOBNAME, "so.table");
		String hivemode = BatchConfig.getDefault(cmap, JOBNAME, "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, JOBNAME, "hive.format");
		HiveClient.saveTable(sotable, hivemode, hiveformat, sodf, "dt", "hh");
		EsClient.saveES(sodf, sotable+"/docs");
		
		//sodf.unpersist();
		return System.currentTimeMillis();
	}
	
	public static long executeCsr(Map<String, Object> cmap
			, String posendmilli, String posenddt, String posendhh
			, String curbeginmilli, String curbegindt, String curbeginhh, String curendmilli
			, String csrinfo_table) {
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame csrdf = hiveExec.executeSQL("csr@selectStatusCsrM"
				, curbegindt, curbeginhh, posenddt, posendhh
				, curbeginmilli, posendmilli, curendmilli, csrinfo_table
				);
		//cssdf.persist();
		
		String csrtable = BatchConfig.getDefault(cmap, JOBNAME, "csr.table");
		String hivemode = BatchConfig.getDefault(cmap, JOBNAME, "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, JOBNAME, "hive.format");
		HiveClient.saveTable(csrtable, hivemode, hiveformat, csrdf, "dt", "hh");
		EsClient.saveES(csrdf, csrtable+"/docs");
		
		//cssdf.unpersist();
		return System.currentTimeMillis();
	}

}

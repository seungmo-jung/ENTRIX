package com.entrixco.cscenter.analysis.batch.job;

import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
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
import org.threeten.bp.LocalDateTime;
import org.threeten.bp.format.DateTimeFormatter;

public class WebJob extends BatchJob {

	private static final Logger logger = LoggerFactory.getLogger(WebJob.class);
//	private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss z yyyy", Locale.US);
//	private static final DateTimeFormatter pFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

	public void execute(JobExecutionContext jobctx) {
		long beginT = System.currentTimeMillis();
		Map<String, Object> cmap = getConfigMap(jobctx);
		
		Date curtime = jobctx.getScheduledFireTime();
		int delay = Integer.parseInt(BatchConfig.getDefault(cmap, "web", "batch.delay"));
		int window = Integer.parseInt(BatchConfig.getDefault(cmap, "web", "batch.window"));
		
		////
		if(cmap.containsKey("window")){
			window = Integer.parseInt(cmap.get("window").toString()) * 60;
			delay = 0;
			logger.info(">>>>>>>>>WINDOW: {}",window);
		}
		////
		
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
			updateCssinfoTmp(cmap, winMin, beginStatMin, "CSS", "web");
		}else{
			cssinfo_table="cssinfo_web";
			String clp = BatchConfig.getDefault(cmap, "web", "cssinfo.local.path");
			String chp = BatchConfig.getDefault(cmap, "web", "cssinfo.hive.path");
			updateCssinfo(cmap, winMin, beginStatMin, clp, chp,"CSS");
		}
		long infoT = System.currentTimeMillis();
		////
		
//		LocalDateTime scheduleTime = LocalDateTime.parse(curtime.toString(), formatter);

		long cssT = executeJob(cmap, "css.table", "web@selectWebCssM", "css"
				,curendmilli,curenddt,curendhh
				,curbeginmilli,curbegindt,curbeginhh
				,cssinfo_table);
		long soT = executeJob(cmap, "so.table", "web@selectWebSoM", "so"
				,curendmilli,curenddt,curendhh
				,curbeginmilli,curbegindt,curbeginhh
				,cssinfo_table);
		long appT = executeJob(cmap, "app.table", "web@selectWebAppM", "app"
				,curendmilli,curenddt,curendhh
				,curbeginmilli,curbegindt,curbeginhh
				,cssinfo_table);
		
		////
		if(cmap.containsKey("window")){
			HiveContext hiveCtx = (HiveContext)cmap.get("hiveCtx");
			hiveCtx.sql("DROP TABLE cssinfo_tmp");
			logger.info("DROP TABLE cssinfo_tmp");
		}
		////
		
		long endT = System.currentTimeMillis();
		logger.info("##################################################");
		logger.info("fire time={}", curtime);
		logger.info("WebJob info time={}, css time={}, so time={}, app time={}, total time={}", (infoT-beginT)/1000.0, (cssT-infoT)/1000.0,
				(soT - cssT) / 1000.0, (appT - soT) / 1000.0, (endT - beginT) / 1000.0);
		logger.info("##################################################");
	}

	public long executeJob(Map<String, Object> cmap, String table, String sql, String type
			, String curendmilli, String curenddt, String curendhh
			, String curbeginmilli, String curbegindt, String curbeginhh
			, String cssinfo_table) {
		
		DataFrame sodf;
		HiveExecutor hiveExec = (HiveExecutor) cmap.get("hiveExec");
		if(type.equals("css")){
			sodf = hiveExec.executeSQL(sql, curenddt, curendhh, curbegindt, curbeginhh,curbeginmilli,curendmilli,cssinfo_table);
		}else{
			sodf = hiveExec.executeSQL(sql, curenddt, curendhh, curbegindt, curbeginhh
					,curbeginmilli.substring(0,16).replaceAll("[-: ]", "")
					,curendmilli.substring(0,16).replaceAll("[-: ]", ""));
		}

		String sotable = BatchConfig.getDefault(cmap, "web", table);
		String hivemode = BatchConfig.getDefault(cmap, "web", "hive.mode");
		HiveClient.saveTable(sotable, hivemode, null, sodf, "dt", "hh");

		logger.info(">>>>>>> {}", sotable);
		EsClient.saveES(sodf, sotable + "/docs");

		return System.currentTimeMillis();
	}

}

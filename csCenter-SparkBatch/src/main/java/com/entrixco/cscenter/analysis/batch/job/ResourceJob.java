package com.entrixco.cscenter.analysis.batch.job;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.batch.client.EsClient;
import com.entrixco.cscenter.analysis.batch.util.BatchConfig;
import com.entrixco.cscenter.analysis.batch.util.HiveExecutor;

public class ResourceJob extends BatchJob {
	
	private static final Logger logger = LoggerFactory.getLogger(ResourceJob.class);
	
	public void execute(JobExecutionContext jobctx) {
		Map<String, Object> cmap = getConfigMap(jobctx);
		int delay = Integer.parseInt(BatchConfig.getDefault(cmap, "stat_pv", "batch.delay"));
		int window = Integer.parseInt(BatchConfig.getDefault(cmap, "stat_pv", "batch.window"));
		
		Date curtime = jobctx.getScheduledFireTime();
		Calendar cal = Calendar.getInstance();
		cal.setTime(curtime);
		cal.add(Calendar.SECOND, -delay);
		Date curend = cal.getTime();
		cal.add(Calendar.SECOND, -window);
		Date curbegin = cal.getTime();
		
		String curenddate = dateForm.format(curend);
		String curbegindate = dateForm.format(curbegin);
		String curenddt = dtForm.format(curend);
		String curbegindt = dtForm.format(curbegin);
		String curendhh = hhForm.format(curend);
		String curbeginhh = hhForm.format(curbegin);
		
		executeForMinute(cmap, curenddate, curbegindate, curenddt, curbegindt, curendhh, curbeginhh);
		
		if(cal.get(Calendar.MINUTE)==0) {
			executeForHour(cmap, curenddate);
		}
		if(cal.get(Calendar.HOUR)==0) {
			executeForDay(cmap, curenddate);
		}
	}
	
	public void executeForMinute(Map<String, Object> cmap
			, String curenddate, String curbegindate
			, String curenddt, String curbegindt, String curendhh, String curbeginhh) {
		
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame df = hiveExec.executeSQL("selectPvByCss"
				, curbegindate.substring(0,16)
				, curbegindt, curbeginhh, curenddt, curendhh, curbegindate, curenddate);
		df.persist();
		
		String hivetable = BatchConfig.getDefault(cmap, "stat_pv", "hive.table");
		String hivemode = BatchConfig.getDefault(cmap, "stat_pv", "hive.mode");
		SaveMode saveMode = hivemode!=null && hivemode.equals("overwrite") ?
				SaveMode.Overwrite : SaveMode.Append;
		df.write().mode(saveMode).partitionBy("dt","hh").saveAsTable(hivetable);
		
		String esindex = BatchConfig.getDefault(cmap, "stat_pv", "es.index");
		EsClient.saveES(df, esindex);
	}
	
	public void executeForHour(Map<String, Object> cmap, String curenddate) {
		
	}
	
	public void executeForDay(Map<String, Object> cmap, String curenddate) {
		
	}

}

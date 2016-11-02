package com.entrixco.cscenter.analysis.batch.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.batch.job.BatchJob;
import com.entrixco.cscenter.analysis.batch.job.SimpleJobExecutionContext;

public class BatchScheduler {
	
	private static final Logger logger = LoggerFactory.getLogger(BatchScheduler.class);
	
	public static void startOnce(HashMap<String, Object> configMap
			, String jobclass, String firetime) {
		try {
			Class<BatchJob> jobclsObj = (Class<BatchJob>)Class.forName(jobclass);
			
			BatchJob batchJob = jobclsObj.newInstance();
			SimpleJobExecutionContext jeCtx = new SimpleJobExecutionContext();
			jeCtx.put(BatchConfig.SCHEDULE_CONFIG_MAP, configMap);
			
			if(firetime!=null && firetime.length()>0) {
				logger.info("job startOnce fire={}", firetime);
				SimpleDateFormat form = new SimpleDateFormat("yyyyMMddHHmmss");
				jeCtx.setFireTime(form.parse(firetime));
			}
			else jeCtx.setFireTime(new Date());
			batchJob.execute(jeCtx);
			
		} catch (Exception e) {
			logger.error("batchScheduler startOnce", e);
			throw new RuntimeException(e);
		}
	}
	
	public static void startSchedule(HashMap<String, Object> configMap
			, String jobclass, String jobname, String jobgroup, String jobcron) {
		try {
			Class<BatchJob> jobclsObj = (Class<BatchJob>)Class.forName(jobclass);
			
			SchedulerFactory schdfac = new StdSchedulerFactory();
			Scheduler schd = schdfac.getScheduler();
			
			
			schd.start();
			schd.getContext().put(BatchConfig.SCHEDULE_CONFIG_MAP, configMap);

			if(jobgroup==null) jobgroup = Scheduler.DEFAULT_GROUP;
			JobDetail jobdet = JobBuilder.newJob(jobclsObj)
					.withIdentity(jobname, jobgroup)
					.build();
			Trigger trg = TriggerBuilder.newTrigger()
					.withIdentity(jobname, jobgroup)
					.startNow()
					.withSchedule(CronScheduleBuilder.cronSchedule(jobcron))
					.build();
		
			schd.scheduleJob(jobdet, trg);
			
		} catch (Exception e) {
			logger.error("batchScheduler startSchedule", e);
			throw new RuntimeException(e);
		}
	}

}

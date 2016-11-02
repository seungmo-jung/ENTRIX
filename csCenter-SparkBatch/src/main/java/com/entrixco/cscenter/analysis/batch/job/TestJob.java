package com.entrixco.cscenter.analysis.batch.job;

import java.text.SimpleDateFormat;
import java.util.Map;

import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJob extends BatchJob {
	
	private static final Logger logger = LoggerFactory.getLogger(TestJob.class);
	
	public void execute(JobExecutionContext jobctx) {
		SimpleDateFormat milliForm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String scheduleTime = milliForm.format(jobctx.getScheduledFireTime());
		logger.info("execute : schedule time={}, hash={}, thread={}"
				, scheduleTime, this.hashCode(), Thread.currentThread().getId());
		
		long bT = System.currentTimeMillis();
		Map<String, Object> cmap = getConfigMap(jobctx);
		BatchJob.copyCssInfoToHive(cmap, jobctx.getScheduledFireTime());
		long eT = System.currentTimeMillis();
		logger.info("##################################################");
		logger.info("times total={}", (eT-bT)/1000.0);
		logger.info("##################################################");
	}

}

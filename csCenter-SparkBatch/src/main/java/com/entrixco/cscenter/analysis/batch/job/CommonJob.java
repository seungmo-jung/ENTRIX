package com.entrixco.cscenter.analysis.batch.job;

import java.util.Date;
import java.util.Map;

import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonJob extends BatchJob {
	
	private static final Logger logger = LoggerFactory.getLogger(CommonJob.class);
	public static final String JOBNAME = "common";
	
	public void execute(JobExecutionContext jobctx) {
		Map<String, Object> cmap = getConfigMap(jobctx);
		Date curtime = jobctx.getScheduledFireTime();
		
		long beginT = System.currentTimeMillis();
		copyCssInfoToHive(cmap, curtime);
		long infoT = System.currentTimeMillis();
		executeDashboard2(cmap, curtime);
		long endT = System.currentTimeMillis();
		
		logger.info("##################################################");
		logger.info("times info={}, total={}"
				, (infoT-beginT)/1000.0, (endT-beginT)/1000.0);
		logger.info("##################################################");
	}

}

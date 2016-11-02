package com.entrixco.cscenter.analysis.batch.job;

import java.util.Date;
import java.util.HashMap;

import org.quartz.Calendar;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerKey;

public class SimpleJobExecutionContext implements JobExecutionContext {
	
	private HashMap<Object, Object> simpleMap = new HashMap<>();
	private Object simpleResult;
	private Date fireTime;
	
	public void setFireTime(Date date) {
		fireTime = date;
	}
	
	@Override public Object get(Object key) {return simpleMap.get(key);}
	@Override public Calendar getCalendar() {return null;}
	@Override public String getFireInstanceId() {return null;}
	@Override public Date getFireTime() {return fireTime;}
	@Override public JobDetail getJobDetail() {return null;}
	@Override public Job getJobInstance() {return null;}
	@Override public long getJobRunTime() {return 0L;}
	@Override public JobDataMap getMergedJobDataMap() {return null;}
	@Override public Date getNextFireTime() {return null;}
	@Override public Date getPreviousFireTime() {return null;}
	@Override public TriggerKey getRecoveringTriggerKey() {return null;}
	@Override public int getRefireCount() {return 0;}
	@Override public Object getResult() {return simpleResult;}
	@Override public Date getScheduledFireTime() {return fireTime;}
	@Override public Scheduler getScheduler() {return null;}
	@Override public Trigger getTrigger() {return null;}
	@Override public boolean isRecovering() {return false;}
	@Override public void put(Object key, Object value) {simpleMap.put(key, value);}
	@Override public void setResult(Object result) {simpleResult = result;}

}

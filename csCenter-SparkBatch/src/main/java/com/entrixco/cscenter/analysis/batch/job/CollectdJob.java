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

public class CollectdJob extends BatchJob {
	
	private static final Logger logger = LoggerFactory.getLogger(CollectdJob.class);
	private static final String JOBNAME = "collectd";
	
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
		String netinfo_table = "";
		int winMin = window/60;
		long beginStatMin = Long.parseLong(curbeginmilli.substring(0,16).replaceAll("[-: ]", ""));
		if(cmap.containsKey("window")){
			cssinfo_table="cssinfo_tmp";
			netinfo_table="netinfo_tmp";
			updateCssinfoTmp(cmap, winMin, beginStatMin, "collectd");
			updateNetinfoTmp(cmap, winMin, beginStatMin);
		}else{
			cssinfo_table="cssinfo_collectd";
			netinfo_table="netinfo_collectd";
			String clp = BatchConfig.getDefault(cmap, JOBNAME, "cssinfo.local.path");
			String chp = BatchConfig.getDefault(cmap, JOBNAME, "cssinfo.hive.path");
			String nlp = BatchConfig.getDefault(cmap, JOBNAME, "netinfo.local.path");
			String nhp = BatchConfig.getDefault(cmap, JOBNAME, "netinfo.hive.path");
			updateCssinfo(cmap, winMin, beginStatMin, clp, chp);
			updateNetinfo(cmap, winMin, beginStatMin, nlp, nhp);
		}
		long infoT = System.currentTimeMillis();
		////
		
		long cpuT = executeCpu(cmap
				, curendmilli, curenddt, curendhh
				, curbeginmilli, curbegindt, curbeginhh,cssinfo_table);
		long memT = executeMem(cmap
				, curendmilli, curenddt, curendhh
				, curbeginmilli, curbegindt, curbeginhh,cssinfo_table);
		long diskT = executeDisk(cmap
				, curendmilli, curenddt, curendhh
				, curbeginmilli, curbegindt, curbeginhh,cssinfo_table);
		long networkT = executeNetwork(cmap
				, curendmilli, curenddt, curendhh
				, curbeginmilli, curbegindt, curbeginhh,netinfo_table);
		long uplinkT = executeUplink(cmap
				, curendmilli, curenddt, curendhh
				, curbeginmilli, curbegindt, curbeginhh,netinfo_table);
		
		////
		if(cmap.containsKey("window")){
			HiveContext hiveCtx = (HiveContext)cmap.get("hiveCtx");
			hiveCtx.sql("DROP TABLE cssinfo_tmp");
			logger.info("DROP TABLE cssinfo_tmp");
			hiveCtx.sql("DROP TABLE netinfo_tmp");
			logger.info("DROP TABLE netinfo_tmp");
		}
		////
		
		long endT = System.currentTimeMillis();
		logger.info("##################################################");
		logger.info("times info={}, cpu={}, mem={}, disk={}, network={}, uplink={}, total={}"
				, (infoT-beginT)/1000.0, (cpuT-infoT)/1000.0, (memT-cpuT)/1000.0, (diskT-memT)/1000.0
				, (networkT-diskT)/1000.0, (uplinkT-networkT)/1000.0, (endT-beginT)/1000.0);
		logger.info("##################################################");
	}
	
	public static long executeCpu(Map<String, Object> cmap
			, String curendmilli, String curenddt, String curendhh
			, String curbeginmilli, String curbegindt, String curbeginhh
			, String info_table) {
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame cpudf = hiveExec.executeSQL("collectd@selectCollectdCpuM"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbeginmilli, curendmilli, info_table
				);
		//cpudf.persist();
		
		String cputable = BatchConfig.getDefault(cmap, JOBNAME, "cpu.table");
		String hivemode = BatchConfig.getDefault(cmap, JOBNAME, "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, JOBNAME, "hive.format");
		HiveClient.saveTable(cputable, hivemode, hiveformat, cpudf, "dt", "hh");
		EsClient.saveES(cpudf, cputable+"/docs");
		
		//cpudf.unpersist();
		return System.currentTimeMillis();
	}
	
	public static long executeMem(Map<String, Object> cmap
			, String curendmilli, String curenddt, String curendhh
			, String curbeginmilli, String curbegindt, String curbeginhh
			, String info_table) {
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame memdf = hiveExec.executeSQL("collectd@selectCollectdMemM"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbeginmilli, curendmilli, info_table
				);
		//memdf.persist();
		
		String memtable = BatchConfig.getDefault(cmap, JOBNAME, "mem.table");
		String hivemode = BatchConfig.getDefault(cmap, JOBNAME, "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, JOBNAME, "hive.format");
		HiveClient.saveTable(memtable, hivemode, hiveformat, memdf, "dt", "hh");
		EsClient.saveES(memdf, memtable+"/docs");
		
		//memdf.unpersist();
		return System.currentTimeMillis();
	}
	
	public static long executeDisk(Map<String, Object> cmap
			, String curendmilli, String curenddt, String curendhh
			, String curbeginmilli, String curbegindt, String curbeginhh
			, String info_table) {
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame diskdf = hiveExec.executeSQL("collectd@selectCollectdDiskM"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbeginmilli, curendmilli, info_table
				);
		//diskdf.persist();
		
		String disktable = BatchConfig.getDefault(cmap, JOBNAME, "disk.table");
		String hivemode = BatchConfig.getDefault(cmap, JOBNAME, "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, JOBNAME, "hive.format");
		HiveClient.saveTable(disktable, hivemode, hiveformat, diskdf, "dt", "hh");
		EsClient.saveES(diskdf, disktable+"/docs");
		
		//diskdf.unpersist();
		return System.currentTimeMillis();
	}
	
	public static long executeNetwork(Map<String, Object> cmap
			, String curendmilli, String curenddt, String curendhh
			, String curbeginmilli, String curbegindt, String curbeginhh
			, String info_table) {
		
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame networkdf = hiveExec.executeSQL("collectd@selectCollectdNetwork2M"
				, curbegindt, curbeginhh, curenddt, curendhh
				, curbeginmilli, curendmilli, info_table
				);
		//networkdf.persist();
		
		String networktable = BatchConfig.getDefault(cmap, JOBNAME, "network.table");
		String hivemode = BatchConfig.getDefault(cmap, JOBNAME, "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, JOBNAME, "hive.format");
		HiveClient.saveTable(networktable, hivemode, hiveformat, networkdf, "dt", "hh");
		EsClient.saveES(networkdf, networktable+"/docs");
		
		//networkdf.unpersist();
		return System.currentTimeMillis();
	}
	
	public static long executeUplink(Map<String, Object> cmap
			, String curendmilli, String curenddt, String curendhh
			, String curbeginmilli, String curbegindt, String curbeginhh
			, String info_table) {
		
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame uplinkdf = hiveExec.executeSQL("collectd@selectCollectdUplink2M"
				, curbegindt, curbeginhh, curenddt, curendhh
				//, curbeginmilli.substring(0,16).replaceAll("[-: ]", "")
				//, curendmilli.substring(0,16).replaceAll("[-: ]", "")
				, curbeginmilli, curendmilli, info_table
				);
		//uplinkdf.persist();
		
		String uplinktable = BatchConfig.getDefault(cmap, JOBNAME, "uplink.table");
		String hivemode = BatchConfig.getDefault(cmap, JOBNAME, "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, JOBNAME, "hive.format");
		HiveClient.saveTable(uplinktable, hivemode, hiveformat, uplinkdf, "dt", "hh");
		EsClient.saveES(uplinkdf, uplinktable+"/docs");
		
		//uplinkdf.unpersist();
		return System.currentTimeMillis();
	}

}

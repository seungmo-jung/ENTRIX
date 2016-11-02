package com.entrixco.cscenter.analysis.batch.job;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.action.search.SearchResponse;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.SchedulerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.batch.client.EsClient;
import com.entrixco.cscenter.analysis.batch.client.HiveClient;
import com.entrixco.cscenter.analysis.batch.client.MysqlClient;
import com.entrixco.cscenter.analysis.batch.util.BatchConfig;
import com.entrixco.cscenter.analysis.batch.util.HiveExecutor;

public abstract class BatchJob implements Job {
	
	private static final Logger logger = LoggerFactory.getLogger(BatchJob.class);
	
	protected SimpleDateFormat milliForm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	protected SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected SimpleDateFormat dtForm = new SimpleDateFormat("yyyyMMdd");
	protected SimpleDateFormat hhForm = new SimpleDateFormat("HH");
	
	protected Map<String, Object> getConfigMap(JobExecutionContext jobCtx) {
		try {
			Object mapobj = jobCtx.get(BatchConfig.SCHEDULE_CONFIG_MAP);
			if(mapobj!=null) return (Map<String, Object>)mapobj;
			
			SchedulerContext schCtx = jobCtx.getScheduler().getContext();
			return (Map<String, Object>)schCtx.get(BatchConfig.SCHEDULE_CONFIG_MAP);
		} catch(Exception e) {
			logger.error("getConfigMap", e);
			throw new RuntimeException(e);
		}
	}
	
	//call only stat_ccu, other job should 5 minute interval
	public static void copyCssInfoToHive(Map<String, Object> cmap, Date curtime) {
//		String cssinfoFireTime = (String)cmap.get("default.cssinfo.firetime");
//		
//		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
//		String curdate = dateFormat.format(curtime);
		SimpleDateFormat dateFormat = new SimpleDateFormat("mm");
		int min = Integer.parseInt(dateFormat.format(curtime));
		logger.info("min: {}, min%5: {}", min, min%5);
		if(min%5!=4) return;
		
		JavaSparkContext jspCtx = (JavaSparkContext)cmap.get("jspCtx");
		MysqlClient mysqlClient = (MysqlClient)cmap.get("mysqlClient");
		mysqlClient.open();
		
		MysqlClient.MysqlData cssdata = mysqlClient.select("aggregateCssInfo");
		String csslpath = (String)cmap.get("default.cssinfo.local.path");
		String csshpath = (String)cmap.get("default.cssinfo.hive.path");
		uploadListToHadoop(cssdata.list, csslpath, csshpath);
		logger.info("**********************************************");
		logger.info("copyCssInfoToHive : count={}, path={}", cssdata.list.size(), csshpath);
		logger.info("**********************************************");
		
		MysqlClient.MysqlData netdata = mysqlClient.select("aggregateNetInfo");
		String netlpath = (String)cmap.get("default.netinfo.local.path");
		String nethpath = (String)cmap.get("default.netinfo.hive.path");
		uploadListToHadoop(netdata.list, netlpath, nethpath);
		logger.info("**********************************************");
		logger.info("copyNetInfoToHive : count={}, path={}", netdata.list.size(), nethpath);
		logger.info("**********************************************");
		
		MysqlClient.MysqlData csrdata = mysqlClient.select("aggregateCsrInfo");
		String csrlpath = (String)cmap.get("default.csrinfo.local.path");
		String csrhpath = (String)cmap.get("default.csrinfo.hive.path");
		uploadListToHadoop(csrdata.list, csrlpath, csrhpath);
		logger.info("**********************************************");
		logger.info("copyCsrInfoToHive : count={}, path={}", csrdata.list.size(), csrhpath);
		logger.info("**********************************************");
	}
	public static void uploadListToHadoop(List<Object[]> list, String lpath, String hpath) {
		PrintWriter writer = null;
		FileSystem hdfsys = null;
		try {
			StringBuilder sb = new StringBuilder();
			for(Object[] arr : list) {
				if(sb.length()>0) sb.append("\n");
				for(int i=0; i<arr.length; i++) {
					if(i>0) sb.append("|");
					sb.append(arr[i]);
				}
			}
			writer = new PrintWriter(new FileWriter(lpath));
			writer.write(sb.toString());
			writer.close();
			writer = null;
			
			Configuration config = new Configuration();
			hdfsys = FileSystem.get(config);
			Path locpath = new Path(lpath);
			Path hdfpath = new Path(hpath);
			hdfsys.copyFromLocalFile(locpath, hdfpath);
			//logger.info(">>>>>> upload local={}, hdfs={}\n{}", lpath, hpath, sb.toString());
			
		} catch(Exception e) {
			logger.error("uploadListToHadoop", e);
			throw new RuntimeException(e);
		} finally {
			if(writer!=null) {
				try {writer.close();} catch(Exception e) {}
			}
			if(hdfsys!=null) {
				try {hdfsys.close();} catch(Exception e) {}
			}
		}
	}
	public static void uploadListToHadoop(StringBuilder sb, String lp, String hp){
		PrintWriter writer = null;
		FileSystem hdfsys = null;
		try {
			writer = new PrintWriter(new FileWriter(lp));
			writer.write(sb.toString());
			writer.close();
			writer = null;
			Configuration config = new Configuration();
			hdfsys = FileSystem.get(config);
			Path locpath = new Path(lp);
			Path hdfpath = new Path(hp);
			hdfsys.copyFromLocalFile(locpath, hdfpath);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void copyCssInfoToHive2(Map<String, Object> cmap, Date curtime) {
		String cssinfoFireTime = (String)cmap.get("default.cssinfo.firetime");
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		String curdate = dateFormat.format(curtime);
		if(!curdate.endsWith(cssinfoFireTime)) return;
		
		JavaSparkContext jspCtx = (JavaSparkContext)cmap.get("jspCtx");
		HiveContext hiveCtx = (HiveContext)cmap.get("hiveCtx");
		MysqlClient mysqlClient = (MysqlClient)cmap.get("mysqlClient");
		mysqlClient.open();
		
		MysqlClient.MysqlData cssdata = mysqlClient.select("aggregateCssInfo");
		List<Object[]> csslist = cssdata.list;
		JavaRDD<Object[]> cssrdd = jspCtx.parallelize(csslist);
		JavaRDD<Row> cssrowrdd = cssrdd.map(new ArrayToRowFunc());
		String[] cssnames = {"app_id","so_id","host_id","host_name","css_ip"};
		StructField[] cssfields = new StructField[cssnames.length];
		for(int f=0; f<cssdata.names.length; f++) {
			cssfields[f] = DataTypes.createStructField(cssnames[f], DataTypes.StringType, true);
		}
		DataFrame cssdf = hiveCtx.createDataFrame(cssrowrdd, new StructType(cssfields));
		cssdf.show();
		String csstable = (String)cmap.get("default.cssinfo.hive.table");
		HiveClient.saveTable(csstable, "overwrite", null, cssdf);
		logger.info("**********************************************");
		logger.info("cssinfo fields : {}", Arrays.toString(cssnames));
		logger.info("copyCssInfoToHive : count={}, table={}", cssdf.count(), csstable);
		logger.info("**********************************************");
		
		MysqlClient.MysqlData netdata = mysqlClient.select("aggregateNetInfo");
		List<Object[]> netlist = netdata.list;
		JavaRDD<Object[]> netrdd = jspCtx.parallelize(netlist);
		JavaRDD<Row> netrowrdd = netrdd.map(new ArrayToRowFunc());
		String[] netnames = {"so_id","switch_id","switch_name","port_id","port_type"};
		StructField[] netfields = new StructField[netnames.length];
		for(int f=0; f<netdata.names.length; f++) {
			netfields[f] = DataTypes.createStructField(netnames[f], DataTypes.StringType, true);
		}
		DataFrame netdf = hiveCtx.createDataFrame(netrowrdd, new StructType(netfields));
		netdf.show();
		String nettable = (String)cmap.get("default.netinfo.hive.table");
		HiveClient.saveTable(nettable, "overwrite", null, netdf);
		logger.info("**********************************************");
		logger.info("netinfo fields : {}", Arrays.toString(netnames));
		logger.info("copyNetInfoToHive : count={}, path={}", netdf.count(), nettable);
		logger.info("**********************************************");
	}
	
	public static class ArrayToRowFunc implements Function<Object[], Row> {
		public Row call(Object[] row) {
			return RowFactory.create(row);
		}
	}
	public static class ArrayToStringFunc implements Function<Object[], String> {
		public String call(Object[] row) {
			StringBuilder sb = new StringBuilder();
			for(Object o : row) {
				if(sb.length()>0) sb.append('|');
				sb.append(o.toString());
			}
			return sb.toString();
		}
	}
	
	public static void executeDashboard(Map<String, Object> cmap, Date beginDate) {
		long startT = System.currentTimeMillis();
		SimpleDateFormat hourFormat = new SimpleDateFormat("yyyyMMddHH");
		int tail = Integer.parseInt((String)cmap.get("default.dashboard.tail"));
		Calendar cal = Calendar.getInstance();
		cal.setTime(beginDate);
		String currhour = hourFormat.format(beginDate);
		String currdt = currhour.substring(0,8);
		String currhh = currhour.substring(8);
		cal.add(Calendar.SECOND, -tail);
		String tailhour = hourFormat.format(cal.getTime());
		String taildt = tailhour.substring(0,8);
		String tailhh = tailhour.substring(8);
		
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame appsodf = hiveExec.executeSQL("selectDashboardAppSo"
				, currdt, currhh, taildt, tailhh
				, currdt, currhh, taildt, tailhh
				, currdt, currhh, taildt, tailhh
				);
		EsClient.saveES(appsodf, "est_so_dashboard/docs", "app_so_id");
		
		DataFrame appdf = appsodf.groupBy("app_id","chg_date")
				.agg(functions.sum("ccu").alias("ccu")
				, functions.sum("uv").alias("uv"), functions.sum("pv").alias("pv")
				, functions.sum("session").alias("session"));
		EsClient.saveES(appdf, "est_app_dashboard/docs", "app_id");
		long endT = System.currentTimeMillis();
		
		logger.info("**********************************************");
		logger.info("dashboard time={}", (endT-startT)/1000.0);
		logger.info("**********************************************");
	}
	
	public static void executeDashboard2(Map<String, Object> cmap, Date beginDate) {
		long startT = System.currentTimeMillis();
		
		EsClient esClient = (EsClient)cmap.get("esClient");
		esClient.open();
		SearchResponse sresp =
				esClient.search("est_app_ccu_min", "docs", "*", 1, "stat_min:desc");
		String ccumax = sresp.getHits().getTotalHits()<=0 ? "000000000000" :
				(String)sresp.getHits().getHits()[0].getSource().get("stat_min");
		sresp = esClient.search("est_app_uvpv_min", "docs", "*", 1, "stat_min:desc");
		String uvpvmax = sresp.getHits().getTotalHits()<=0 ? "000000000000" :
				(String)sresp.getHits().getHits()[0].getSource().get("stat_min");
		sresp = esClient.search("est_app_bandwidth_min", "docs", "*", 1, "stat_min:desc");
		String trionmax = sresp.getHits().getTotalHits()<=0 ? "000000000000" :
				(String)sresp.getHits().getHits()[0].getSource().get("stat_min");
		logger.info("**********************************************");
		logger.info("dashboard2 ccu={}, uvpv={}, traffic={}", ccumax, uvpvmax, trionmax);
		logger.info("**********************************************");
		
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame appsodf = hiveExec.executeSQL("selectDashboardAppSo2"
				, ccumax.substring(0,8), ccumax.substring(8,10), ccumax
				, uvpvmax.substring(0,8), uvpvmax.substring(8,10), uvpvmax
				, trionmax.substring(0,8), trionmax.substring(8,10), trionmax
				);
		EsClient.saveES(appsodf, "est_so_dashboard/docs", "app_so_id");
		
		DataFrame appdf = appsodf.groupBy("app_id","chg_date")
				.agg(functions.sum("ccu").alias("ccu")
				, functions.sum("uv").alias("uv")
				, functions.sum("pv").alias("pv")
				, functions.sum("traffic").alias("traffic"));
		EsClient.saveES(appdf, "est_app_dashboard/docs", "app_id");
		
		long endT = System.currentTimeMillis();
		logger.info("**********************************************");
		logger.info("dashboard2 time={}", (endT-startT)/1000.0);
		logger.info("**********************************************");
	}
	public static void executeDashboard3(Map<String, Object> cmap, Date beginDate) {
		long startT = System.currentTimeMillis();
		
		EsClient esClient = (EsClient)cmap.get("esClient");
		esClient.open();
		logger.info("**********************************************");
		logger.info("dashboard3 start");
		logger.info("**********************************************");
		
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame appsodf = hiveExec.executeSQL("selectDashboardAppSo3");
		EsClient.saveES(appsodf, "est_so_dashboard/docs", "app_so_id");
		
		DataFrame appdf = appsodf.groupBy("app_id","chg_date")
				.agg(functions.sum("ccu").alias("ccu")
						, functions.sum("uv").alias("uv")
						, functions.sum("pv").alias("pv")
						, functions.sum("traffic").alias("traffic"));
		EsClient.saveES(appdf, "est_app_dashboard/docs", "app_id");
		
		long endT = System.currentTimeMillis();
		logger.info("**********************************************");
		logger.info("dashboard3 time={}", (endT-startT)/1000.0);
		logger.info("**********************************************");
	}
	
	public static void updateCssinfo(Map<String, Object> cmap, int winMin, Long beginStatMin, String localPath, String hivePath){
		String cssinfo = "select stat_min, app_id, so_id, host_name, css_ip from (";
		for(int i=0; i<winMin; i++){
			Long statMin = beginStatMin + i;
			cssinfo += " select '"+statMin+"' stat_min, app_id, so_id, host_name, css_ip  from tb_cssinfo union all";
		}
		cssinfo = cssinfo.substring(0, cssinfo.length()-"union all".length());
		cssinfo += ") C";
		
		HiveContext hiveCtx = (HiveContext)cmap.get("hiveCtx");
		DataFrame cssdf = hiveCtx.sql(cssinfo);
		
		StringBuilder csb = new StringBuilder();
		if(cssdf.count()>0){
			Row[] row = cssdf.collect();
			int col = 5;
			for(Row r :row){
				for(int i=0; i<col; i++){
					if(i!=col-1){
						csb.append(r.get(i)+"|");
					}else{
						csb.append(r.get(i)+"\n");
					}
				}
			}
		}else{
			csb.setLength(0);
		}
		uploadListToHadoop(csb,localPath,hivePath);
	}
	public static void updateCssinfo(Map<String, Object> cmap, int winMin, Long beginStatMin, String localPath, String hivePath,String type){
		String cssinfo = "select stat_min, app_id, so_id, host_name, css_ip from (";
		for(int i=0; i<winMin; i++){
			Long statMin = beginStatMin + i;
			cssinfo += " select '"+statMin+"' stat_min, app_id, so_id, host_name, css_ip  from tb_cssinfo where role_typ='"+type+"' union all";
		}
		cssinfo = cssinfo.substring(0, cssinfo.length()-"union all".length());
		cssinfo += ") C";
		
		HiveContext hiveCtx = (HiveContext)cmap.get("hiveCtx");
		DataFrame cssdf = hiveCtx.sql(cssinfo);
		
		StringBuilder csb = new StringBuilder();
		if(cssdf.count()>0){
			Row[] row = cssdf.collect();
			int col = 5;
			for(Row r :row){
				for(int i=0; i<col; i++){
					if(i!=col-1){
						csb.append(r.get(i)+"|");
					}else{
						csb.append(r.get(i)+"\n");
					}
				}
			}
		}else{
			csb.setLength(0);
		}
		uploadListToHadoop(csb,localPath,hivePath);
	}
	public static void updateNetinfo(Map<String, Object> cmap, int winMin, Long beginStatMin, String localPath, String hivePath){
		String cssinfo = "select stat_min, so_id, switch_id, switch_name, port_id, port_type from (";
		for(int i=0; i<winMin; i++){
			Long statMin = beginStatMin + i;
			cssinfo += " select '"+statMin+"' stat_min, so_id, switch_id, switch_name, port_id, port_type  from tb_netinfo union all";
		}
		cssinfo = cssinfo.substring(0, cssinfo.length()-"union all".length());
		cssinfo += ") C";
		
		HiveContext hiveCtx = (HiveContext)cmap.get("hiveCtx");
		DataFrame netdf = hiveCtx.sql(cssinfo);
		
		StringBuilder csb = new StringBuilder();
		if(netdf.count()>0){
			Row[] row = netdf.collect();
			int col = 6;
			for(Row r :row){
				for(int i=0; i<col; i++){
					if(i!=col-1){
						csb.append(r.get(i)+"|");
					}else{
						csb.append(r.get(i)+"\n");
					}
				}
			}
		}else{
			csb.setLength(0);
		}
		uploadListToHadoop(csb,localPath,hivePath);
	}
	public static void updateCsrinfo(Map<String, Object> cmap, int winMin, Long beginStatMin, String localPath, String hivePath){
		String cssinfo = "select stat_min, so_id, host_id, csr_port, csr_id from (";
		for(int i=0; i<winMin; i++){
			Long statMin = beginStatMin + i;
			cssinfo += " select '"+statMin+"' stat_min, so_id, host_id, csr_port, csr_id  from tb_csrinfo union all";
		}
		cssinfo = cssinfo.substring(0, cssinfo.length()-"union all".length());
		cssinfo += ") C";
		
		HiveContext hiveCtx = (HiveContext)cmap.get("hiveCtx");
		DataFrame csrdf = hiveCtx.sql(cssinfo);
		
		StringBuilder csb = new StringBuilder();
		if(csrdf.count()>0){
			Row[] row = csrdf.collect();
			int col = 5;
			for(Row r :row){
				for(int i=0; i<col; i++){
					if(i!=col-1){
						csb.append(r.get(i)+"|");
					}else{
						csb.append(r.get(i)+"\n");
					}
				}
			}
		}else{
			csb.setLength(0);
			csb.append(" ");
		}
		uploadListToHadoop(csb,localPath,hivePath);
	}
	
	//TMP INFO
	public static void updateCssinfoTmp(Map<String, Object> cmap, int winMin, Long beginStatMin,String type,String job){
		String cssinfo = "select stat_min, app_id, so_id, host_name, css_ip from (";
		Long statMin = (long)0;
		for(int i=0; i<winMin; i++){
			statMin = beginStatMin + i;
			cssinfo += " select '"+statMin+"' stat_min, app_id, so_id, host_name, css_ip  from tb_cssinfo where role_typ='"+type+"' union all";
		}
		cssinfo = cssinfo.substring(0, cssinfo.length()-"union all".length());
		cssinfo += ") C";
		
		HiveContext hiveCtx = (HiveContext)cmap.get("hiveCtx");
		hiveCtx.sql("DROP TABLE cssinfo_tmp");
		hiveCtx.sql("CREATE TABLE cssinfo_tmp( "
				+ "stat_min STRING,"
				+ "app_id STRING,"
				+ "so_id STRING,"
				+ "host_name STRING,"
				+ "css_ip STRING"
				+ ")"
				+ "row format delimited fields terminated by '|' lines terminated by '\n'"
				+ "LOCATION 'hdfs://entrixcluster/table/cssinfo_tmp'");
		DataFrame csrRouteCss = hiveCtx.sql(cssinfo);
		
		csrRouteCss.write().mode(SaveMode.Append).saveAsTable("cssinfo_tmp");
		
		String mode = cmap.get("mode").toString();
		if(mode.equals("H")){
			String[] codeA = {"app","so","css"};
			EsClient esClient = (EsClient)cmap.get("esClient");
			esClient.open();
			if(job.equals("tps")){
				String[] tpsA = {"csr_tps","so_csrtps","web_tps","so_webtps"};
				for(String tps : tpsA){
					String index = "est_"+tps+"_min";
					esClient.delete(index, "docs", beginStatMin+"", statMin+"");
					hiveCtx.sql("ALTER TABLE "+index+" DROP IF EXISTS PARTITION (dt='"+statMin.toString().substring(0,8)+"', hh='"+statMin.toString().substring(8,10)+"')");
				}
			}else{
				for(String code : codeA){
					String index = "est_"+code+"_"+job+"_min";
					esClient.delete(index, "docs", beginStatMin+"", statMin+"");
					hiveCtx.sql("ALTER TABLE "+index+" DROP IF EXISTS PARTITION (dt='"+statMin.toString().substring(0,8)+"', hh='"+statMin.toString().substring(8,10)+"')");
				}
				if(job.equals("route") || job.equals("status")){
					String index = "est_csr_"+job+"_min";
					esClient.delete(index, "docs", beginStatMin+"", statMin+"");
					hiveCtx.sql("ALTER TABLE "+index+" DROP IF EXISTS PARTITION (dt='"+statMin.toString().substring(0,8)+"', hh='"+statMin.toString().substring(8,10)+"')");
				}
			}
		}
	}
	public static void updateCssinfoTmp(Map<String, Object> cmap, int winMin, Long beginStatMin,String job){
		String cssinfo = "select stat_min, app_id, so_id, host_name, css_ip from (";
		Long statMin = (long)0;
		for(int i=0; i<winMin; i++){
			statMin = beginStatMin + i;
			cssinfo += " select '"+statMin+"' stat_min, app_id, so_id, host_name, css_ip  from tb_cssinfo union all";
		}
		cssinfo = cssinfo.substring(0, cssinfo.length()-"union all".length());
		cssinfo += ") C";
		
		HiveContext hiveCtx = (HiveContext)cmap.get("hiveCtx");
		hiveCtx.sql("DROP TABLE cssinfo_tmp");
		hiveCtx.sql("CREATE TABLE cssinfo_tmp( "
				+ "stat_min STRING,"
				+ "app_id STRING,"
				+ "so_id STRING,"
				+ "host_name STRING,"
				+ "css_ip STRING"
				+ ")"
				+ "row format delimited fields terminated by '|' lines terminated by '\n'"
				+ "LOCATION 'hdfs://entrixcluster/table/cssinfo_tmp'");
		DataFrame csrRouteCss = hiveCtx.sql(cssinfo);
		
		csrRouteCss.write().mode(SaveMode.Append).saveAsTable("cssinfo_tmp");
		
		String mode = cmap.get("mode").toString();
		if(mode.equals("H")){
			String[] colA = {"cpu","mem","disk","network","uplink"};
			EsClient esClient = (EsClient)cmap.get("esClient");
			esClient.open();
			
			for(String col : colA){
				String index = "est_"+col+"_min";
				esClient.delete(index, "docs", beginStatMin+"", statMin+"");
				hiveCtx.sql("ALTER TABLE "+index+" DROP IF EXISTS PARTITION (dt='"+statMin.toString().substring(0,8)+"', hh='"+statMin.toString().substring(8,10)+"')");
			}
		}
	}
	public static void updateNetinfoTmp(Map<String, Object> cmap, int winMin, Long beginStatMin){
		String cssinfo = "select stat_min, so_id, switch_id, switch_name, port_id, port_type from (";
		for(int i=0; i<winMin; i++){
			Long statMin = beginStatMin + i;
			cssinfo += " select '"+statMin+"' stat_min, so_id, switch_id, switch_name, port_id, port_type  from tb_netinfo union all";
		}
		cssinfo = cssinfo.substring(0, cssinfo.length()-"union all".length());
		cssinfo += ") C";
		
		HiveContext hiveCtx = (HiveContext)cmap.get("hiveCtx");
		hiveCtx.sql("DROP TABLE netinfo_tmp");
		hiveCtx.sql("CREATE TABLE netinfo_tmp("
				+ "stat_min STRING,"
				+ "so_id STRING,"
				+ "switch_id STRING,"
				+ "switch_name STRING,"
				+ "port_id STRING,"
				+ "port_type STRING"
				+ ")"
				+ "row format delimited fields terminated by '|' lines terminated by '\n'"
				+ "LOCATION 'hdfs://entrixcluster/table/netinfo_tmp'");
		DataFrame csrRouteCss = hiveCtx.sql(cssinfo);
		csrRouteCss.write().mode(SaveMode.Append).saveAsTable("netinfo_tmp");
	}
	public static void updateCsrinfoTmp(Map<String, Object> cmap, int winMin, Long beginStatMin){
		String cssinfo = "select stat_min, so_id, host_id, csr_port, csr_id from (";
		for(int i=0; i<winMin; i++){
			Long statMin = beginStatMin + i;
			cssinfo += " select '"+statMin+"' stat_min, so_id, host_id, csr_port, csr_id  from tb_csrinfo union all";
		}
		cssinfo = cssinfo.substring(0, cssinfo.length()-"union all".length());
		cssinfo += ") C";
		
		HiveContext hiveCtx = (HiveContext)cmap.get("hiveCtx");
		hiveCtx.sql("DROP TABLE csrinfo_tmp");
		hiveCtx.sql("CREATE TABLE csrinfo_tmp("
				+ "stat_min STRING,"
				+ "so_id STRING,"
				+ "host_id STRING,"
				+ "csr_port STRING,"
				+ "csr_id STRING"
				+ ")"
				+ "row format delimited fields terminated by '|' lines terminated by '\n'"
				+ "LOCATION 'hdfs://entrixcluster/table/csrinfo_tmp'");
		DataFrame csrRouteCss = hiveCtx.sql(cssinfo);
		csrRouteCss.write().mode(SaveMode.Append).saveAsTable("csrinfo_tmp");
	}
}

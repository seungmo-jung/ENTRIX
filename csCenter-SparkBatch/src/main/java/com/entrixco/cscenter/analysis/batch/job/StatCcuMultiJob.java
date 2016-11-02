package com.entrixco.cscenter.analysis.batch.job;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.batch.client.EsClient;
import com.entrixco.cscenter.analysis.batch.client.HiveClient;
import com.entrixco.cscenter.analysis.batch.util.BatchConfig;
import com.entrixco.cscenter.analysis.batch.util.HiveExecutor;

import scala.Tuple2;

public class StatCcuMultiJob extends BatchJob {
	
	private static final Logger logger = LoggerFactory.getLogger(StatCcuMultiJob.class);
	
	public static class Ccu implements Serializable {
		public String cssip;
		public String appid;
		public String soid;
		public int[][] minutes;
		public Ccu(String css, String app, String so, int[][] mins) {
			cssip = css; appid = app; soid = so; minutes = mins;
		}
		public Ccu(String app, String so, int[][] mins) {
			appid = app; soid = so; minutes = mins;
		}
		public Ccu(String app, int[][] mins) {
			appid = app; minutes = mins;
		}
	}
	public static final int _CCU_KEY = 0
			, _CCU_REGDATE = 1, _CCU_MOTION = 2, _CCU_OFFDATE = 3
			, _CCU_APP = 4, _CCU_SO = 5, _CCU_CSS = 6
			, _CCU_MIN = 7, _CCU_APP_KEY = 8, _CCU_SO_KEY = 9;
	public static final String[] CSS_FIELDS = {
			"stat_min", "app_id", "so_id", "css_ip",
			"css_ccu", "avg_ccu", "min_ccu", "max_ccu",
			"chg_date",
			"dt", "hh"
			};
	public static final String[] APP_FIELDS = {
			"stat_min", "app_id",
			"css_ccu", "avg_ccu", "min_ccu", "max_ccu",
			"chg_date",
			"dt", "hh"
	};
	public static final String[] SO_FIELDS = {
			"stat_min", "app_id", "so_id",
			"css_ccu", "avg_ccu", "min_ccu", "max_ccu",
			"chg_date",
			"dt", "hh"
	};
	public static final DataType[] CSS_TYPES = {
			DataTypes.StringType, DataTypes.StringType, DataTypes.StringType, DataTypes.StringType,
			DataTypes.StringType, DataTypes.DoubleType, DataTypes.IntegerType, DataTypes.IntegerType,
			DataTypes.StringType,
			DataTypes.StringType, DataTypes.StringType
			};
	public static final DataType[] APP_TYPES = {
			DataTypes.StringType, DataTypes.StringType,
			DataTypes.StringType, DataTypes.DoubleType, DataTypes.IntegerType, DataTypes.IntegerType,
			DataTypes.StringType,
			DataTypes.StringType, DataTypes.StringType
	};
	public static final DataType[] SO_TYPES = {
			DataTypes.StringType, DataTypes.StringType, DataTypes.StringType,
			DataTypes.StringType, DataTypes.DoubleType, DataTypes.IntegerType, DataTypes.IntegerType,
			DataTypes.StringType,
			DataTypes.StringType, DataTypes.StringType
	};
	
	public void execute(JobExecutionContext jobctx) {
		long beginT = System.currentTimeMillis();
		Map<String, Object> cmap = getConfigMap(jobctx);
		int delay = Integer.parseInt(BatchConfig.getDefault(cmap, "stat_ccu", "batch.delay"));
		int poswindow = Integer.parseInt(BatchConfig.getDefault(cmap, "stat_ccu", "batch.poswindow"));
		int window = Integer.parseInt(BatchConfig.getDefault(cmap, "stat_ccu", "batch.window"));
		int prewindow = Integer.parseInt(BatchConfig.getDefault(cmap, "stat_ccu", "batch.prewindow"));
		
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
		cal.add(Calendar.SECOND, -poswindow);
		Date curend = cal.getTime();
		cal.add(Calendar.SECOND, -window);
		Date curbegin = cal.getTime();
		cal.add(Calendar.SECOND, -prewindow);
		Date prebegin = cal.getTime();
		
		String curenddate = dateForm.format(curend);
		String curbegindate = dateForm.format(curbegin);
		String curenddt = dtForm.format(curend);
		String curendhh = hhForm.format(curend);
		
		String prebegindate = dateForm.format(prebegin);
		String prebegindt = dtForm.format(prebegin);
		String prebeginhh = hhForm.format(prebegin);
		
		String chgdate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+09:00").format(curtime);
		
		////
		String cssinfo_table = "";
		int winMin = window/60;
		long beginStatMin = Long.parseLong(curbegindate.substring(0,16).replaceAll("[-: ]", ""));
		if(cmap.containsKey("window")){
			cssinfo_table="cssinfo_tmp";
			updateCssinfoTmp(cmap, winMin, beginStatMin, "CSS", "ccu");
		}else{
			cssinfo_table="cssinfo_stat_ccu";
			String clp = BatchConfig.getDefault(cmap, "stat_ccu", "cssinfo.local.path");
			String chp = BatchConfig.getDefault(cmap, "stat_ccu", "cssinfo.hive.path");
			updateCssinfo(cmap, winMin, beginStatMin, clp, chp,"CSS");
		}
		long infoT = System.currentTimeMillis();
		////
		
		String minStatmin = curbegindate.substring(0,16).replaceAll("[-: ]", "");
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame alldf = hiveExec.executeSQL("stat@selectStatCcuMultiM"
				, minStatmin, minStatmin
				, prebegindt, prebeginhh, curenddt, curendhh
				, prebegindate, curenddate, curbegindate, cssinfo_table
				);
		long collectT = System.currentTimeMillis();
		
		JavaPairRDD<String, Row> allrdd = alldf.javaRDD().mapToPair(new KeyPairFunc2("CSS"));
		JavaPairRDD<String, Iterable<Row>> listrdd = allrdd.groupByKey();
		JavaPairRDD<String, Ccu> ccurdd = listrdd.mapToPair(
				new KeyRowToCssCcuFunc2(window, curbegin));
		JavaPairRDD<String, Ccu> cssrdd = ccurdd.reduceByKey(new ReduceCcuFunc2());
		JavaRDD<Row> cssrowrdd = cssrdd.flatMap(new CssRowFunc2(curbegin, chgdate));
		HiveContext hiveCtx = (HiveContext)cmap.get("hiveCtx");
		StructField[] fields = new StructField[CSS_FIELDS.length];
		for(int f=0; f<CSS_FIELDS.length; f++) {
			fields[f] = DataTypes.createStructField(CSS_FIELDS[f], CSS_TYPES[f], true);
		}
		DataFrame cssdf = hiveCtx.createDataFrame(cssrowrdd, new StructType(fields));

		cssdf.persist();
		long cssCnt = 0;//cssdf.count();
		long cssT = System.currentTimeMillis();
		
		String csstable = BatchConfig.getDefault(cmap, "stat_ccu", "css.table");
		String hivemode = BatchConfig.getDefault(cmap, "stat_ccu", "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, "stat_ccu", "hive.format");
		
		HiveClient.saveTable(csstable, hivemode, hiveformat, cssdf, "dt", "hh");
		long csshiveT = System.currentTimeMillis();
		EsClient.saveES(cssdf, csstable+"/docs");
		long cssesT = System.currentTimeMillis();
		
		String apptable = BatchConfig.getDefault(cmap, "stat_ccu", "app.table");
//		DataFrame appdf = cssdf.groupBy("stat_min","app_id","dt","hh","chg_date")
//				.agg(functions.avg("avg_ccu").alias("avg_ccu")
//						, functions.max("max_ccu").alias("max_ccu")
//						, functions.min("min_ccu").alias("min_ccu"))
//				.select("stat_min","app_id","avg_ccu","min_ccu","max_ccu","chg_date","dt","hh");
		//appdf.persist();
		JavaPairRDD<String, Row> approw = alldf.javaRDD().mapToPair(new KeyPairFunc2("APP"));
		JavaPairRDD<String, Iterable<Row>> listapp = approw.groupByKey();
		JavaPairRDD<String, Ccu> ccuapp = listapp.mapToPair(
				new KeyRowToAppCcuFunc2(window, curbegin));
		JavaPairRDD<String, Ccu> apprdd = ccuapp.reduceByKey(new ReduceCcuFunc2());
		JavaRDD<Row> approwrdd = apprdd.flatMap(new AppRowFunc2(curbegin, chgdate));
		StructField[] appfields = new StructField[APP_FIELDS.length];
		for(int f=0; f<APP_FIELDS.length; f++) {
			appfields[f] = DataTypes.createStructField(APP_FIELDS[f], APP_TYPES[f], true);
		}
		DataFrame appdf = hiveCtx.createDataFrame(approwrdd, new StructType(appfields));
		
		long appCnt = 0;
		long appT = System.currentTimeMillis();
		HiveClient.saveTable(apptable, hivemode, hiveformat, appdf, "dt", "hh");
		long apphiveT = System.currentTimeMillis();
		EsClient.saveES(appdf, apptable+"/docs");
		long appesT = System.currentTimeMillis();
		
		String sotable = BatchConfig.getDefault(cmap, "stat_ccu", "so.table");
//		DataFrame sodf = cssdf.groupBy("stat_min","app_id","so_id","dt","hh","chg_date")
//				.agg(functions.avg("avg_ccu").alias("avg_ccu")
//						, functions.max("max_ccu").alias("max_ccu")
//						, functions.min("min_ccu").alias("min_ccu"))
//				.select("stat_min","app_id","so_id","avg_ccu","min_ccu","max_ccu","chg_date","dt","hh");
		//sodf.persist();
		JavaPairRDD<String, Row> sorow = alldf.javaRDD().mapToPair(new KeyPairFunc2("SO"));
		JavaPairRDD<String, Iterable<Row>> listso = sorow.groupByKey();
		JavaPairRDD<String, Ccu> ccuso = listso.mapToPair(
				new KeyRowToSoCcuFunc2(window, curbegin));
		JavaPairRDD<String, Ccu> sordd = ccuso.reduceByKey(new ReduceCcuFunc2());
		JavaRDD<Row> sorowrdd = sordd.flatMap(new SoRowFunc2(curbegin, chgdate));
		StructField[] sofields = new StructField[SO_FIELDS.length];
		for(int f=0; f<SO_FIELDS.length; f++) {
			sofields[f] = DataTypes.createStructField(SO_FIELDS[f], SO_TYPES[f], true);
		}
		DataFrame sodf = hiveCtx.createDataFrame(sorowrdd, new StructType(sofields));
		long soCnt = 0;
		long soT = System.currentTimeMillis();
		HiveClient.saveTable(sotable, hivemode, hiveformat, sodf, "dt", "hh");
		long sohiveT = System.currentTimeMillis();
		EsClient.saveES(sodf, sotable+"/docs");
		long soesT = System.currentTimeMillis();
		
		cssdf.unpersist();
		//appdf.unpersist();
		//sodf.unpersist();
		
		////
		if(cmap.containsKey("window")){
			hiveCtx.sql("DROP TABLE cssinfo_tmp");
			logger.info("DROP TABLE cssinfo_tmp");
		}
		////
		
		long endT = System.currentTimeMillis();
		
		logger.info("##################################################");
		logger.info("info time={}, collect={}"
				+ ", cssT({})={}, cchiveT={}, cssesT={}"
				+ ", appT({})={}, apphiveT={}, appesT={}"
				+ ", soT({})={}, sohiveT={}, soesT={}"
				+ ", total={}"
				, (infoT-beginT)/1000.0, (collectT-infoT)/1000.0
				, cssCnt, (cssT-collectT)/1000.0, (csshiveT-cssT)/1000.0, (cssesT-csshiveT)/1000.0
				, appCnt, (appT-cssesT)/1000.0, (apphiveT-appT)/1000.0, (appesT-apphiveT)/1000.0
				, soCnt, (soT-appesT)/1000.0, (sohiveT-soT)/1000.0, (soesT-sohiveT)/1000.0
				, (endT-beginT)/1000.0);
		logger.info("##################################################");
		
		//executeDashboard2(cmap, curbegin);
	}
	
	public static class KeyPairFunc2 implements PairFunction<Row, String, Row> {
		private String type;
		public KeyPairFunc2(String type){
			this.type = type;
		}
		public Tuple2<String, Row> call(Row row) {
			String key="";
			if(type.equals("CSS")){
				key = row.getString(_CCU_KEY);
			}else if(type.equals("APP")){
				key = row.getString(_CCU_APP_KEY);
			}else if(type.equals("SO")){
				key = row.getString(_CCU_SO_KEY);
			}
			return new Tuple2<String, Row>(key, row);
		};
	}
	
	public static class KeyRowToCssCcuFunc2
			implements PairFunction<Tuple2<String, Iterable<Row>>, String, Ccu> {
		private int window;
		private Date beginDate;
		public KeyRowToCssCcuFunc2(int window, Date begindate) {
			this.window = window;
			this.beginDate = begindate;
		}
		public Tuple2<String, Ccu> call(Tuple2<String, Iterable<Row>> rowpair) {
			long begintime = beginDate.getTime();
			long endtime = begintime + window*1000;
			int mincnt = window/60;
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			String appid = null, soid = null, cssip = null;
			long ontime = 0, offtime = 0;
			int onsec = 0, offsec = 0;
			int[][] mins = new int[mincnt][60];
			try {
				for(Row row : rowpair._2) {
					
					if(appid==null) appid = row.getString(_CCU_APP);
					if(soid==null) soid = row.getString(_CCU_SO);
					if(cssip==null) cssip = row.getString(_CCU_CSS);
					
					ontime = dateFormat.parse(row.getString(_CCU_REGDATE)).getTime();
					if(ontime<begintime) mins[0][0] = 1;
					else {
						onsec = (int)(ontime-begintime)/1000;
						if(onsec/60<mincnt && onsec/60 >= 0){
							mins[onsec/60][onsec%60] = 1;
						}else{
							offtime = dateFormat.parse(row.getString(_CCU_MIN)).getTime();
							offsec = (int)(offtime-begintime)/1000;
							mins[offsec/60][offsec%60] = -1;
						}
					}
					offtime = dateFormat.parse(row.getString(_CCU_OFFDATE)).getTime();
					if(offtime>=endtime) ;
					else {
						offsec = (int)(offtime-begintime)/1000;
						if(offsec/60<mincnt && offsec/60 >= 0){
							mins[offsec/60][offsec%60] = -1;
						}else{
							offtime = dateFormat.parse(row.getString(_CCU_MIN)).getTime();
							offsec = (int)(offtime-begintime)/1000;
							mins[offsec/60][offsec%60] = -1;
						}
					}
				}
			} catch(Exception e) {
				logger.error("KeyRowToCssCcuFunc2", e);
				throw new RuntimeException(e);
			}
			int pre = 0;
			for(int m=0; m<mincnt; m++) {
				for(int s=0; s<60; s++) {
					switch(mins[m][s]) {
					case 1: pre = 1; break;
					case -1: mins[m][s] = 0; pre = 0; break;
					case 0: if(pre==1) mins[m][s] = 1; break;
					}
				}
			}
			Ccu ccu = new Ccu(cssip, appid, soid, mins);
			return new Tuple2<String, Ccu>(cssip, ccu);
		}
	}
	public static class KeyRowToSoCcuFunc2
			implements PairFunction<Tuple2<String, Iterable<Row>>, String, Ccu> {
		private int window;
		private Date beginDate;
		public KeyRowToSoCcuFunc2(int window, Date begindate) {
			this.window = window;
			this.beginDate = begindate;
		}
		public Tuple2<String, Ccu> call(Tuple2<String, Iterable<Row>> rowpair) {
			long begintime = beginDate.getTime();
			long endtime = begintime + window*1000;
			int mincnt = window/60;
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			String appid = null, soid = null;
			long ontime = 0, offtime = 0;
			int onsec = 0, offsec = 0;
			int[][] mins = new int[mincnt][60];
			try {
				for(Row row : rowpair._2) {
					
					if(appid==null) appid = row.getString(_CCU_APP);
					if(soid==null) soid = row.getString(_CCU_SO);
					
					ontime = dateFormat.parse(row.getString(_CCU_REGDATE)).getTime();
					if(ontime<begintime) mins[0][0] = 1;
					else {
						onsec = (int)(ontime-begintime)/1000;
						if(onsec/60<mincnt && onsec/60 >= 0){
							mins[onsec/60][onsec%60] = 1;
						}else{
							offtime = dateFormat.parse(row.getString(_CCU_MIN)).getTime();
							offsec = (int)(offtime-begintime)/1000;
							mins[offsec/60][offsec%60] = -1;
						}
					}
					offtime = dateFormat.parse(row.getString(_CCU_OFFDATE)).getTime();
					if(offtime>=endtime) ;
					else {
						offsec = (int)(offtime-begintime)/1000;
						if(offsec/60<mincnt && offsec/60 >= 0){
							mins[offsec/60][offsec%60] = -1;
						}else{
							offtime = dateFormat.parse(row.getString(_CCU_MIN)).getTime();
							offsec = (int)(offtime-begintime)/1000;
							mins[offsec/60][offsec%60] = -1;
						}
					}
				}
			} catch(Exception e) {
				logger.error("KeyRowToCssCcuFunc2", e);
				throw new RuntimeException(e);
			}
			int pre = 0;
			for(int m=0; m<mincnt; m++) {
				for(int s=0; s<60; s++) {
					switch(mins[m][s]) {
					case 1: pre = 1; break;
					case -1: mins[m][s] = 0; pre = 0; break;
					case 0: if(pre==1) mins[m][s] = 1; break;
					}
				}
			}
			Ccu ccu = new Ccu(appid, soid, mins);
			return new Tuple2<String, Ccu>(soid, ccu);
		}
	}
	public static class KeyRowToAppCcuFunc2
	implements PairFunction<Tuple2<String, Iterable<Row>>, String, Ccu> {
		private int window;
		private Date beginDate;
		public KeyRowToAppCcuFunc2(int window, Date begindate) {
			this.window = window;
			this.beginDate = begindate;
		}
		public Tuple2<String, Ccu> call(Tuple2<String, Iterable<Row>> rowpair) {
			long begintime = beginDate.getTime();
			long endtime = begintime + window*1000;
			int mincnt = window/60;
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			String appid = null;
			long ontime = 0, offtime = 0;
			int onsec = 0, offsec = 0;
			int[][] mins = new int[mincnt][60];
			try {
				for(Row row : rowpair._2) {
					
					if(appid==null) appid = row.getString(_CCU_APP);
					
					ontime = dateFormat.parse(row.getString(_CCU_REGDATE)).getTime();
					if(ontime<begintime) mins[0][0] = 1;
					else {
						onsec = (int)(ontime-begintime)/1000;
						if(onsec/60<mincnt && onsec/60 >= 0){
							mins[onsec/60][onsec%60] = 1;
						}else{
							offtime = dateFormat.parse(row.getString(_CCU_MIN)).getTime();
							offsec = (int)(offtime-begintime)/1000;
							mins[offsec/60][offsec%60] = -1;
						}
					}
					offtime = dateFormat.parse(row.getString(_CCU_OFFDATE)).getTime();
					if(offtime>=endtime) ;
					else {
						offsec = (int)(offtime-begintime)/1000;
						if(offsec/60<mincnt && offsec/60 >= 0){
							mins[offsec/60][offsec%60] = -1;
						}else{
							offtime = dateFormat.parse(row.getString(_CCU_MIN)).getTime();
							offsec = (int)(offtime-begintime)/1000;
							mins[offsec/60][offsec%60] = -1;
						}
					}
				}
			} catch(Exception e) {
				logger.error("KeyRowToCssCcuFunc2", e);
				throw new RuntimeException(e);
			}
			int pre = 0;
			for(int m=0; m<mincnt; m++) {
				for(int s=0; s<60; s++) {
					switch(mins[m][s]) {
					case 1: pre = 1; break;
					case -1: mins[m][s] = 0; pre = 0; break;
					case 0: if(pre==1) mins[m][s] = 1; break;
					}
				}
			}
			Ccu ccu = new Ccu(appid, mins);
			return new Tuple2<String, Ccu>(appid, ccu);
		}
	}
	
	public static class ReduceCcuFunc2 implements Function2<Ccu, Ccu, Ccu> {
		public Ccu call(Ccu c1, Ccu c2) {
			int[][] newmins = new int[c1.minutes.length][60];
			for(int m=0; m<newmins.length; m++) {
				for(int s=0; s<60; s++) newmins[m][s] = c1.minutes[m][s] + c2.minutes[m][s];
			}
			c1.minutes = newmins;
			return c1;
		}
	}
	
	public static class CssRowFunc2 implements FlatMapFunction<Tuple2<String, Ccu>, Row> {
		private Date beginDate;
		private String chgdate;
		public CssRowFunc2(Date beginDate, String chgdate) {
			this.beginDate = beginDate;
			this.chgdate = chgdate;
		}
		//"stat_date", "app_id", "so_id", "css_ip"
		//, "css_ccu", "avg_ccu", "min_ccu", "max_ccu"
		//, "chg_date"
		//, "dt", "hh"
		public Iterable<Row> call(Tuple2<String, Ccu> tuple) {
			ArrayList<Row> rowlist = new ArrayList<>();
			int[][] mins = tuple._2.minutes;
			SimpleDateFormat minFormat = new SimpleDateFormat("yyyyMMddHHmm");
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(beginDate);
			
			for(int m=0; m<mins.length; m++) {
				Object[] row = new Object[CSS_FIELDS.length];
				String statmin = minFormat.format(calendar.getTime());
				row[0] = statmin;
				row[1] = tuple._2.appid;
				row[2] = tuple._2.soid;
				row[3] = tuple._2.cssip;
				int[] seconds = mins[m];
				double sum = 0;
				int min = Integer.MAX_VALUE, max = 0;
				StringBuilder sb = new StringBuilder();
				for(int t=0; t<seconds.length; t++) {
					if(t>0) sb.append(',');
					sb.append(seconds[t]);
					
					sum += seconds[t];
					if(seconds[t]<min) min = seconds[t];
					if(seconds[t]>max) max = seconds[t];
				}
				row[4] = sb.toString();
				row[5] = sum/seconds.length;
				row[6] = min;
				row[7] = max;
				row[8] = chgdate;
				row[9] = statmin.substring(0,8);
				row[10] = statmin.substring(8,10);
				rowlist.add(RowFactory.create(row));
				
				calendar.add(Calendar.MINUTE, 1);
			}
			return rowlist;
		}
	}
	public static class SoRowFunc2 implements FlatMapFunction<Tuple2<String, Ccu>, Row> {
		private Date beginDate;
		private String chgdate;
		public SoRowFunc2(Date beginDate, String chgdate) {
			this.beginDate = beginDate;
			this.chgdate = chgdate;
		}
		//"stat_date", "app_id", "so_id"
		//, "css_ccu", "avg_ccu", "min_ccu", "max_ccu"
		//, "chg_date"
		//, "dt", "hh"
		public Iterable<Row> call(Tuple2<String, Ccu> tuple) {
			ArrayList<Row> rowlist = new ArrayList<>();
			int[][] mins = tuple._2.minutes;
			SimpleDateFormat minFormat = new SimpleDateFormat("yyyyMMddHHmm");
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(beginDate);
			
			for(int m=0; m<mins.length; m++) {
				Object[] row = new Object[SO_FIELDS.length];
				String statmin = minFormat.format(calendar.getTime());
				row[0] = statmin;
				row[1] = tuple._2.appid;
				row[2] = tuple._2.soid;
				int[] seconds = mins[m];
				double sum = 0;
				int min = Integer.MAX_VALUE, max = 0;
				StringBuilder sb = new StringBuilder();
				for(int t=0; t<seconds.length; t++) {
					if(t>0) sb.append(',');
					sb.append(seconds[t]);
					
					sum += seconds[t];
					if(seconds[t]<min) min = seconds[t];
					if(seconds[t]>max) max = seconds[t];
				}
				row[3] = sb.toString();
				row[4] = sum/seconds.length;
				row[5] = min;
				row[6] = max;
				row[7] = chgdate;
				row[8] = statmin.substring(0,8);
				row[9] = statmin.substring(8,10);
				rowlist.add(RowFactory.create(row));
				
				calendar.add(Calendar.MINUTE, 1);
			}
			return rowlist;
		}
	}
	public static class AppRowFunc2 implements FlatMapFunction<Tuple2<String, Ccu>, Row> {
		private Date beginDate;
		private String chgdate;
		public AppRowFunc2(Date beginDate, String chgdate) {
			this.beginDate = beginDate;
			this.chgdate = chgdate;
		}
		//"stat_date", "app_id"
		//, "css_ccu", "avg_ccu", "min_ccu", "max_ccu"
		//, "chg_date"
		//, "dt", "hh"
		public Iterable<Row> call(Tuple2<String, Ccu> tuple) {
			ArrayList<Row> rowlist = new ArrayList<>();
			int[][] mins = tuple._2.minutes;
			SimpleDateFormat minFormat = new SimpleDateFormat("yyyyMMddHHmm");
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(beginDate);
			
			for(int m=0; m<mins.length; m++) {
				Object[] row = new Object[APP_FIELDS.length];
				String statmin = minFormat.format(calendar.getTime());
				row[0] = statmin;
				row[1] = tuple._2.appid;
				int[] seconds = mins[m];
				double sum = 0;
				int min = Integer.MAX_VALUE, max = 0;
				StringBuilder sb = new StringBuilder();
				for(int t=0; t<seconds.length; t++) {
					if(t>0) sb.append(',');
					sb.append(seconds[t]);
					
					sum += seconds[t];
					if(seconds[t]<min) min = seconds[t];
					if(seconds[t]>max) max = seconds[t];
				}
				row[2] = sb.toString();
				row[3] = sum/seconds.length;
				row[4] = min;
				row[5] = max;
				row[6] = chgdate;
				row[7] = statmin.substring(0,8);
				row[8] = statmin.substring(8,10);
				rowlist.add(RowFactory.create(row));
				
				calendar.add(Calendar.MINUTE, 1);
			}
			return rowlist;
		}
	}

}

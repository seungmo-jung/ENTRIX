package com.entrixco.cscenter.analysis.batch.job;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
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

//notice : 30minute delay
public class StatCcuSingleJob extends BatchJob {
	
	private static final Logger logger = LoggerFactory.getLogger(StatCcuSingleJob.class);
	private static final String ST_FORMAT = "yyyyMMddHHmm";
	private static final String ES_FORMAT = "yyyy-MM-dd'T'HH:mm:ss+09:00";
	
	public static class Ccu implements Serializable {
		public String cssip;
		public String appid;
		public String soid;
		public int[] seconds;
		public Row preRow;
		public Row posRow;
		public Ccu(String css, String app, String so, int[] sec, Row pre, Row pos) {
			cssip = css; appid = app; soid = so; seconds = sec; preRow = pre; posRow = pos;
		}
	}
	public static final int _CCU_KEY = 0
			, _CCU_REGDATE = 1, _CCU_MOTION = 2, _CCU_OFFDATE = 3
			, _CCU_APP = 4, _CCU_SO = 5, _CCU_CSS = 6;
	public static final String[] CSS_FIELDS = {
			"stat_min", "app_id", "so_id", "css_ip",
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
	
	public void execute(JobExecutionContext jobctx) {
		long beginT = System.currentTimeMillis();
		Map<String, Object> cmap = getConfigMap(jobctx);
		int delay = Integer.parseInt(BatchConfig.getDefault(cmap, "stat_ccu", "batch.delay"));
		int poswindow = Integer.parseInt(BatchConfig.getDefault(cmap, "stat_ccu", "batch.poswindow"));
		int window = 60;
		int prewindow = Integer.parseInt(BatchConfig.getDefault(cmap, "stat_ccu", "batch.prewindow"));
		Date curtime = jobctx.getScheduledFireTime();
		Calendar cal = Calendar.getInstance();
		cal.setTime(curtime);
		cal.add(Calendar.SECOND, -delay);
		cal.add(Calendar.SECOND, -poswindow);
		Date curend = cal.getTime();
		cal.add(Calendar.SECOND, -window);
		Date curbegin = cal.getTime();
		Date preend = curbegin;
		cal.add(Calendar.SECOND, -prewindow);
		Date prebegin = cal.getTime();
		
		String curenddate = dateForm.format(curend);
		String curbegindate = dateForm.format(curbegin);
		String curenddt = dtForm.format(curend);
		String curbegindt = dtForm.format(curbegin);
		String curendhh = hhForm.format(curend);
		String curbeginhh = hhForm.format(curbegin);
		
		String statmin = new SimpleDateFormat(ST_FORMAT).format(curbegin);
		
		String preenddate = dateForm.format(preend);
		String prebegindate = dateForm.format(prebegin);
		String preenddt = dtForm.format(preend);
		String prebegindt = dtForm.format(prebegin);
		String preendhh = hhForm.format(preend);
		String prebeginhh = hhForm.format(prebegin);
		
		if(curenddate.endsWith(":00:00")) {
			//copyCssInfoToHive(jobctx);
		}
		
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame alldf = hiveExec.executeSQL("stat@selectStatCcuSingle"
				, prebegindt, prebeginhh, curenddt, curendhh
				, curbegindate, curenddate, curbegindate, curenddate
				);
		long collectT = System.currentTimeMillis();
		Date chgdate = new Date(collectT);
		String esdate = new SimpleDateFormat(ES_FORMAT).format(chgdate);
		
		JavaPairRDD<String, Row> allrdd = alldf.javaRDD().mapToPair(new KeyPairFunc2());
		JavaPairRDD<String, Iterable<Row>> listrdd = allrdd.groupByKey();
		JavaPairRDD<String, Ccu> ccurdd = listrdd.mapToPair(
				new KeyRowToCssCcuFunc2(curbegindate));
		JavaPairRDD<String, Ccu> cssrdd = ccurdd.reduceByKey(new ReduceCssFunc2());
		JavaRDD<Row> cssrowrdd = cssrdd.map(new CssRowFunc2(statmin, esdate));
		
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
		long hiveT1 = System.currentTimeMillis();
		EsClient.saveES(cssdf, csstable+"/docs");
		long esT1 = System.currentTimeMillis();
		
		String apptable = BatchConfig.getDefault(cmap, "stat_ccu", "app.table");
		DataFrame appdf = cssdf.groupBy("stat_min","app_id","dt","hh","chg_date")
				.agg(functions.avg("avg_ccu").alias("avg_ccu")
						, functions.max("max_ccu").alias("max_ccu")
						, functions.min("min_ccu").alias("min_ccu"))
				.select("stat_min","app_id","avg_ccu","min_ccu","max_ccu","chg_date","dt","hh");
		//appdf.persist();
		long appCnt = 0;
		long appT = System.currentTimeMillis();
		HiveClient.saveTable(apptable, hivemode, hiveformat, appdf, "dt", "hh");
		long hiveT2 = System.currentTimeMillis();
		EsClient.saveES(appdf, apptable+"/docs");
		long esT2 = System.currentTimeMillis();
		
		String sotable = BatchConfig.getDefault(cmap, "stat_ccu", "so.table");
		DataFrame sodf = cssdf.groupBy("stat_min","app_id","so_id","dt","hh","chg_date")
				.agg(functions.avg("avg_ccu").alias("avg_ccu")
						, functions.max("max_ccu").alias("max_ccu")
						, functions.min("min_ccu").alias("min_ccu"))
				.select("stat_min","app_id","so_id","avg_ccu","min_ccu","max_ccu","chg_date","dt","hh");
		//sodf.persist();
		long soCnt = 0;
		long soT = System.currentTimeMillis();
		HiveClient.saveTable(sotable, hivemode, hiveformat, sodf, "dt", "hh");
		long hiveT3 = System.currentTimeMillis();
		EsClient.saveES(sodf, sotable+"/docs");
		long esT3 = System.currentTimeMillis();
		
		cssdf.unpersist();
		//appdf.unpersist();
		//sodf.unpersist();
		long endT = System.currentTimeMillis();
		
		logger.info("##################################################");
		logger.info("collect={}"
				+ ", cssT({})={}, hiveT1={}, esT1={}"
				+ ", appT({})={}, hiveT2={}, esT2={}"
				+ ", soT({})={}, hiveT3={}, esT3={}"
				+ ", total={}"
				, (collectT-beginT)/1000.0
				, cssCnt, (cssT-beginT)/1000.0, (hiveT1-beginT)/1000.0, (esT1-beginT)/1000.0
				, appCnt, (appT-beginT)/1000.0, (hiveT2-beginT)/1000.0, (esT2-beginT)/1000.0
				, soCnt, (soT-beginT)/1000.0, (hiveT3-beginT)/1000.0, (esT3-beginT)/1000.0
				, (endT-beginT)/1000.0);
		logger.info("##################################################");
		
		//executeDashboard2(cmap, curbegin);
	}
	
	public static class KeyPairFunc2 implements PairFunction<Row, String, Row> {
		public Tuple2<String, Row> call(Row row) {
			String key = row.getString(_CCU_KEY);
			return new Tuple2<String, Row>(key, row);
		};
	}
	
	public static class KeyRowToCssCcuFunc2
			implements PairFunction<Tuple2<String, Iterable<Row>>, String, Ccu> {
		private int window;
		private SimpleDateFormat dateFormat;
		private String begindate;
		public KeyRowToCssCcuFunc2(String begindate) {
			this.window = 60;
			this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			this.begindate = begindate;
		}
		public Tuple2<String, Ccu> call(Tuple2<String, Iterable<Row>> rowpair) {
			String cssip = rowpair._1.split("_")[0];
			String appid = null;
			String soid = null;
			//String motion = null;
			
			String ondate = null, offdate = null;
			long ontime = 0, offtime = 0;
			int[] ccus = new int[window];
			try {
				long begintime = dateFormat.parse(begindate).getTime();
				long endtime = begintime + window*1000;
				for(Row row : rowpair._2) {
					if(appid==null) appid = row.getString(_CCU_APP);
					if(soid==null) soid = row.getString(_CCU_SO);
					//motion = row.getString(_CCU_MOTION);
					
					ondate = row.getString(_CCU_REGDATE);
					ontime = dateFormat.parse(ondate).getTime();
					if(ontime<begintime) ccus[0] = 1;
					else ccus[Integer.parseInt(ondate.substring(17,19))] = 1;
					
					offdate = row.getString(_CCU_OFFDATE);
					offtime = dateFormat.parse(offdate).getTime();
					if(offtime>=endtime) ;
					else ccus[Integer.parseInt(offdate.substring(17,19))] = -1;
				}
			} catch(Exception e) {
				throw new RuntimeException(e);
			}
			int pre = 0;
			for(int t=0; t<window; t++) {
				switch(ccus[t]) {
				case 1: pre = 1; break;
				case -1: ccus[t] = 0; pre = 0; break;
				case 0: if(pre==1) ccus[t] = 1; break;
				}
			}
			Ccu ccu = new Ccu(cssip, appid, soid, ccus, null, null);
			return new Tuple2<String, Ccu>(cssip, ccu);
		}
	}
	
	public static class ReduceCssFunc2 implements Function2<Ccu, Ccu, Ccu> {
		public Ccu call(Ccu c1, Ccu c2) {
			int[] news = new int[c1.seconds.length];
			for(int t=0; t<news.length; t++) news[t] = c1.seconds[t] + c2.seconds[t];
			c1.seconds = news;
			return c1;
		}
	}
	
	public static class CssRowFunc2 implements Function<Tuple2<String, Ccu>, Row> {
		private String statMin;
		private Object chgDate;
		public CssRowFunc2(String statMin, Object chgDate) {
			this.statMin = statMin;
			this.chgDate = chgDate;
		}
		//"stat_date", "app_id", "so_id", "css_ip"
		//, "css_ccu", "avg_ccu", "min_ccu", "max_ccu"
		//, "chg_date"
		//, "dt", "hh"
		public Row call(Tuple2<String, Ccu> tuple) {
			Object[] row = new Object[CSS_FIELDS.length];
			row[0] = statMin;
			row[1] = tuple._2.appid;
			row[2] = tuple._2.soid;
			row[3] = tuple._2.cssip;
			int[] seconds = tuple._2.seconds;
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
			row[8] = chgDate;
			row[9] = statMin.substring(0,8);
			row[10] = statMin.substring(8,10);
			return RowFactory.create(row);
		}
	}

}

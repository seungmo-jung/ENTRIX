package com.entrixco.cscenter.analysis.batch.job;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
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

public class StatCcuJob extends BatchJob {
	
	private static final Logger logger = LoggerFactory.getLogger(StatCcuJob.class);

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
	
	public static final int _CCU_KEY = 0, _CCU_REGDATE = 1, _CCU_MOTION = 2
			, _CCU_SO = 3, _CCU_APP = 4, _CCU_CSS = 5, _CCU_FLAG = 6;
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
		int window = Integer.parseInt(BatchConfig.getDefault(cmap, "stat_ccu", "batch.window"));
		int prewindow = Integer.parseInt(BatchConfig.getDefault(cmap, "stat_ccu", "batch.prewindow"));
		int poswindow = Integer.parseInt(BatchConfig.getDefault(cmap, "stat_ccu", "batch.poswindow"));
		Date curtime = jobctx.getScheduledFireTime();
		Calendar cal = Calendar.getInstance();
		cal.setTime(curtime);
		cal.add(Calendar.SECOND, -delay);
		
		Date posend = cal.getTime();
		cal.add(Calendar.SECOND, -poswindow);
		Date posbegin = cal.getTime();
		cal.add(Calendar.SECOND, -window);
		Date preend = cal.getTime();
		cal.add(Calendar.SECOND, -prewindow);
		Date prebegin = cal.getTime();
		
		String posenddate = dateForm.format(posend);
		String posbegindate = dateForm.format(posbegin);
		String preenddate = dateForm.format(preend);
		String prebegindate = dateForm.format(prebegin);
		
		String posenddt = dtForm.format(posend);
		String posbegindt = dtForm.format(posbegin);
		String preenddt = dtForm.format(preend);
		String prebegindt = dtForm.format(posbegin);
		
		String posendhh = hhForm.format(posend);
		String posbeginhh = hhForm.format(posbegin);
		String preendhh = hhForm.format(preend);
		String prebeginhh = hhForm.format(prebegin);
		
		String curenddate = posbegindate;
		String curbegindate = preenddate;
		String curenddt = posbegindt;
		String curbegindt = preenddt;
		String curendhh = posbeginhh;
		String curbeginhh = preendhh;
		
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		
		Map<String, Row> posMin = poswindow==0 ? new HashMap<String, Row>() :
			getPostMinimum(hiveExec, posbegindt, posbeginhh, posenddt, posendhh, posbegindate, posenddate);
		Map<String, Row> preMax = prewindow==0 ? new HashMap<String, Row>() :
			getPrevMaximum(hiveExec, prebegindt, prebeginhh, preenddt, preendhh, prebegindate, preenddate);
		
		DataFrame curdf = hiveExec.executeSQL("stat@selectStatCcu"
				, curbegindt, curbeginhh, curenddt, curendhh, curbegindate, curenddate);
		Map<String, Ccu> secondsMap = getSecondsGroup(curdf, window).collectAsMap();
		long collectT = System.currentTimeMillis();
		
		Map<String, Ccu> fullmap = new HashMap<>();
		Row row = null;
		Ccu ccu = null;
		for(String key : posMin.keySet()) {
			row = posMin.get(key);
			if(fullmap.containsKey(key)) fullmap.get(key).posRow = row;
			else fullmap.put(key, new Ccu(row.getString(_CCU_CSS)
					, row.getString(_CCU_APP), row.getString(_CCU_SO), null, null, posMin.get(key)));
		}
		for(String key : secondsMap.keySet()) {
			ccu = secondsMap.get(key);
			if(fullmap.containsKey(key)) fullmap.get(key).seconds = secondsMap.get(key).seconds;
			else fullmap.put(key, ccu);
		}
		for(String key : preMax.keySet()) {
			row = preMax.get(key);
			if(fullmap.containsKey(key)) fullmap.get(key).posRow = row;
			else fullmap.put(key, new Ccu(row.getString(_CCU_CSS)
					, row.getString(_CCU_APP), row.getString(_CCU_SO), null, preMax.get(key), null));
		}
		
		JavaSparkContext jspCtx = (JavaSparkContext)cmap.get("jspCtx");
		JavaRDD<Ccu> fullrdd = jspCtx.parallelize(new ArrayList<>(fullmap.values()));
		JavaPairRDD<String, Ccu> secrdd = fullrdd.mapToPair(
				new SecondsToCssFunc(preend, prewindow, window, poswindow));
		JavaPairRDD<String, Ccu> cssrdd = secrdd.reduceByKey(new ReduceCssFunc());
		JavaRDD<Row> cssrowrdd = cssrdd.map(new CssRowFunc(curbegindate));
		
		HiveContext hiveCtx = (HiveContext)cmap.get("hiveCtx");
		StructField[] fields = new StructField[CSS_FIELDS.length];
		for(int f=0; f<CSS_FIELDS.length; f++) {
			fields[f] = DataTypes.createStructField(CSS_FIELDS[f], CSS_TYPES[f], true);
		}
		DataFrame cssdf = hiveCtx.createDataFrame(cssrowrdd, new StructType(fields));
		cssdf.persist();
		long createT = System.currentTimeMillis();
		//cssdf.show(100);
		
		String csstable = BatchConfig.getDefault(cmap, "stat_ccu", "css.table");
		String hivemode = BatchConfig.getDefault(cmap, "stat_ccu", "hive.mode");
		String hiveformat = BatchConfig.getDefault(cmap, "stat_ccu", "hive.format");
		
		HiveClient.saveTable(csstable, hivemode, hiveformat, cssdf, "dt", "hh");
		long hiveT1 = System.currentTimeMillis();
		EsClient.saveES(cssdf, csstable+"/docs");
		long esT1 = System.currentTimeMillis();
		
		String apptable = BatchConfig.getDefault(cmap, "stat_ccu", "app.table");
		DataFrame appdf = cssdf.groupBy("stat_min", "so_id", "app_id", "dt", "hh")
				.agg(functions.avg("avg_ccu").alias("avg_ccu")
						, functions.max("max_ccu").alias("max_ccu")
						, functions.min("min_ccu").alias("min_ccu"))
				.select("stat_date","so_id","app_id","avg_ccu","max_ccu","min_ccu","chg_date","dt","hh");
		//appdf.show(100);
		HiveClient.saveTable(apptable, hivemode, hiveformat, appdf, "dt", "hh");
		long hiveT2 = System.currentTimeMillis();
		EsClient.saveES(appdf, apptable+"/docs");
		long esT2 = System.currentTimeMillis();
		
		String sotable = BatchConfig.getDefault(cmap, "stat_ccu", "so.table");
		DataFrame sodf = cssdf.groupBy("stat_min", "so_id", "dt", "hh")
				.agg(functions.avg("avg_ccu").alias("avg_ccu")
						, functions.max("max_ccu").alias("max_ccu")
						, functions.min("min_ccu").alias("min_ccu"))
				.select("stat_min","so_id","avg_ccu","max_ccu","min_ccu","chg_date","dt","hh");
		HiveClient.saveTable(sotable, hivemode, hiveformat, sodf, "dt", "hh");
		long hiveT3 = System.currentTimeMillis();
		//sodf.show(100);
		EsClient.saveES(sodf, sotable+"/docs");
		long esT3 = System.currentTimeMillis();
		
		cssdf.unpersist();
		long endT = System.currentTimeMillis();
		
		logger.info("##################################################");
		logger.info("collect={}, create={}, hiveT1={}, esT1={}, hiveT2={}, esT2={}, hiveT3={}, esT3={}, total={}"
				, (collectT-beginT)/1000.0, (createT-beginT)/1000.0
				, (hiveT1-beginT)/1000.0, (esT1-beginT)/1000.0
				, (hiveT2-beginT)/1000.0, (esT2-beginT)/1000.0
				, (hiveT3-beginT)/1000.0, (esT3-beginT)/1000.0
				, (endT-beginT)/1000.0);
		logger.info("##################################################");
		
	}
	
	protected Map<String, Row> getPostMinimum(HiveExecutor hiveExec
			, String posbegindt, String posbeginhh, String posenddt
			, String posendhh, String posbegindate, String posenddate) {
		DataFrame posdf = hiveExec.executeSQL("selectStatCcu"
				, posbegindt, posbeginhh, posenddt, posendhh, posbegindate, posenddate);
		return getLastGroup(posdf, false).collectAsMap();
	}
	protected Map<String, Row> getPrevMaximum(HiveExecutor hiveExec
			, String prebegindt, String prebeginhh, String preenddt
			, String preendhh, String prebegindate, String preenddate) {
		DataFrame predf = hiveExec.executeSQL("selectStatCcu"
				, prebegindt, prebeginhh, preenddt, preendhh, prebegindate, preenddate);
		return getLastGroup(predf, true).collectAsMap();
	}
	protected JavaPairRDD<String, Row> getLastGroup(DataFrame df, boolean isMax) {
		JavaPairRDD<String, Row> prdd = df.javaRDD().mapToPair(new KeyPairFunc());
		logger.info("prdd count={}", prdd.count());
		JavaPairRDD<String, Iterable<Row>> iterrdd = prdd.groupByKey();
		JavaPairRDD<String, Row> lastrdd = iterrdd.mapValues(new GetLastFunc(isMax));
		//lastrdd.count();
		return lastrdd;
	}
	protected JavaPairRDD<String, Ccu> getSecondsGroup(DataFrame df, int window) {
		JavaPairRDD<String, Row> prdd = df.javaRDD().mapToPair(new KeyPairFunc());
		logger.info("prdd count={}", prdd.count());
		JavaPairRDD<String, Iterable<Row>> iterrdd = prdd.groupByKey();
		JavaPairRDD<String, Ccu> ccurdd = iterrdd.mapValues(new GetSecondsFunc(window));
		//ordrdd.count();
		return ccurdd;
	}
	
	public static class KeyPairFunc implements PairFunction<Row, String, Row> {
		public Tuple2<String, Row> call(Row row) {
			String key = row.getString(_CCU_KEY);
			//logger.info("key={}, rdate={}, motion={}", key, row.getString(1), row.getString(2));
			return new Tuple2<String, Row>(key, row);
		};
	}
	public static class GetLastFunc implements Function<Iterable<Row>, Row> {
		private boolean isMax;
		public GetLastFunc(boolean isMax) {
			this.isMax = isMax;
		}
		public Row call(Iterable<Row> rows) {
			String tardate = null, curdate = null;
			Row tarrow = null;
			int cnt = 0;
			for(Row row : rows) {
				curdate = row.getString(_CCU_REGDATE);
				if(tardate==null 
						|| (isMax ? tardate.compareTo(curdate)<=0 : tardate.compareTo(curdate)>0)) {
					tardate = curdate; tarrow = row;
				}
				cnt++;
			}
			//logger.info("{}({}) key={}, rdate={}, motion={}", isMax ? "max":"min"
			//		, cnt, tarrow.getString(0), tarrow.getString(1), tarrow.getString(2));
			return tarrow;
		}
	}
	public static class GetSecondsFunc implements Function<Iterable<Row>, Ccu> {
		private int window;
		public GetSecondsFunc(int window) {
			this.window = window;
		}
		public Ccu call(Iterable<Row> rows) {
			int[] onoffs = new int[window];
			String regdate = null, motion = null, soid = null, appid = null, cssip = null;
			int seconds = 0;
			for(Row row : rows) {
				regdate = row.getString(_CCU_REGDATE);
				motion = row.getString(_CCU_MOTION);
				if(soid==null) soid = row.getString(_CCU_SO);
				if(appid==null) appid = row.getString(_CCU_APP);
				if(cssip==null) cssip = row.getString(_CCU_CSS);
				if(motion.equals("ON")) {
					seconds = Integer.parseInt(regdate.substring(regdate.length()-2));
					onoffs[seconds] = 1;
				}
				else {
					seconds = Integer.parseInt(regdate.substring(regdate.length()-2));
					onoffs[seconds] = -1;
				}
			}
			return new Ccu(cssip, appid, soid, onoffs, null, null);
		}
	}
	public static class SecondsToCssFunc implements PairFunction<Ccu, String, Ccu> {
		private Date beginDate;
		private int window, preWindow, posWindow;
		public SecondsToCssFunc(Date beginDate, int preWindow, int window, int posWindow) {
			this.beginDate = beginDate;
			this.window = window;
			this.preWindow = preWindow;
			this.posWindow = posWindow;
		}
		public Tuple2<String, Ccu> call(Ccu ccu) {
			if(ccu.seconds==null) ccu.seconds = new int[window];
			String preOn = ccu.preRow==null
					|| !ccu.preRow.getString(_CCU_MOTION).equals("ON") ?
							null : ccu.preRow.getString(_CCU_REGDATE);
			String posOff = ccu.posRow==null 
					|| !ccu.posRow.getString(_CCU_MOTION).equals("OFF") ?
							null : ccu.posRow.getString(_CCU_REGDATE);
			calculate(ccu.seconds, beginDate, preWindow, window, posWindow, preOn, posOff);
			
			return new Tuple2<>(ccu.cssip, ccu);
		}
	}
	public static void calculate(int[] seconds
			, Date beginDate, int preWindow, int window, int posWindow, String preOn, String posOff) {
		try {
			long begintime = beginDate.getTime();
			long endtime = begintime + window*1000;
			SimpleDateFormat regdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			long premaxtime = preOn==null ?
					0 : regdf.parse(preOn).getTime() + preWindow*1000;
			//System.out.println("begintime="+begintime+", premaxtime="+premaxtime);
			if(premaxtime>begintime) {
				int premax = (int)(premaxtime-begintime)/1000;
				//System.out.println("premax="+premax);
				for(int t=0; t<window && t<premax; t++) {
					if(seconds[t]!=0) break;
					seconds[t] = 1;
				}
			}
			long posmintime = posOff==null ?
					Long.MAX_VALUE : regdf.parse(posOff).getTime() - posWindow*1000;
			//System.out.println("posmintime="+posmintime+", endtimetime="+endtime);
			if(posmintime<=endtime) {
				int posmin = (int)(posmintime-begintime)/1000;
				//System.out.println("posmin="+posmin);
				for(int t=(window-1); t>=0 && t>=posmin; t--) {
					if(seconds[t]!=0) break;
					seconds[t] = 1;
				}
			}
			int pre = 0;
			for(int t=0; t<window; t++) {
				switch(seconds[t]) {
				case 1: pre = 1; break;
				case -1: if(posWindow==0) seconds[t] = 0; pre = 0; break;
				case 0: if(pre==1) seconds[t] = 1; break;
				}
			}
			if(posWindow>0) {
				int pos = 0;
				for(int t=window-1; t>=0; t--) {
					switch(seconds[t]) {
					case 1: pos = 0; break;
					case -1: seconds[t] = 0; pos = 1; break;
					case 0: if(pos==1) seconds[t] = 1; break;
					}
				}
			}
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	public static class ReduceCssFunc implements Function2<Ccu, Ccu, Ccu> {
		public Ccu call(Ccu c1, Ccu c2) {
			int[] news = new int[c1.seconds.length];
			for(int t=0; t<news.length; t++) news[t] = c1.seconds[t] + c2.seconds[t];
			c1.seconds = news;
			return c1;
		}
	}
	public static class CssRowFunc implements Function<Tuple2<String, Ccu>, Row> {
		private String begindate;
		private Date curDate;
		public CssRowFunc(String begindate) {
			this.begindate = begindate;
			this.curDate = new Date(System.currentTimeMillis());
		}
		//"stat_date", "so_id", "app_id", "css_ip"
		//, "css_ccu", "avg_ccu", "max_ccu", "min_ccu"
		//, "chg_date"
		//, "dt", "hh"
		public Row call(Tuple2<String, Ccu> tuple) {
			Object[] row = new Object[CSS_FIELDS.length];
			row[0] = begindate.substring(0,16).replaceAll("[-: ]", "");
			row[1] = tuple._2.soid;
			row[2] = tuple._2.appid;
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
			row[6] = max;
			row[7] = min;
			row[8] = curDate;
			row[9] = begindate.substring(0,10).replace("-", "");
			row[10] = begindate.substring(11,13);
			return RowFactory.create(row);
		}
	}
	
	public static class ForeachRow implements VoidFunction<Tuple2<String, Iterable<Row>>> {
		private int rdateidx, moidx;
		public ForeachRow(HashMap<String, Integer> colmap) {
			this.rdateidx = colmap.get("registered_date");
			this.moidx = colmap.get("motion");
		}
		public void call(Tuple2<String, Iterable<Row>> t2) {
			int cnt = 0;
			boolean inorder = true;
			String rdate = null, pdate = null;
			for(Row row : t2._2) {
				rdate = row.getString(rdateidx);
				if(pdate!=null && pdate.compareTo(rdate)>=0) inorder = false;
				pdate = rdate;
				cnt++;
			}
			logger.info("foreach key={}, size={}, inorder={}", t2._1, cnt, inorder);
		}
	}

}

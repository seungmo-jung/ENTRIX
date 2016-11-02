package com.entrixco.cscenter.analysis.batch.job;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.hive.HiveContext;
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

public class StatCcuDistJob extends StatCcuJob {
	
	private static final Logger logger = LoggerFactory.getLogger(StatCcuDistJob.class);
	private static final String ES_FORMAT = "yyyy-MM-dd'T'HH:mm:ss+09:00";
	
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
		String posenddt = dtForm.format(posend);
		String posbegindt = dtForm.format(posbegin);
		String posendhh = hhForm.format(posend);
		String posbeginhh = hhForm.format(posbegin);
		
		String preenddate = dateForm.format(preend);
		String prebegindate = dateForm.format(prebegin);
		String preenddt = dtForm.format(preend);
		String prebegindt = dtForm.format(posbegin);
		String preendhh = hhForm.format(preend);
		String prebeginhh = hhForm.format(prebegin);
		
		String curenddate = posbegindate;
		String curbegindate = preenddate;
		String curenddt = posbegindt;
		String curbegindt = preenddt;
		String curendhh = posbeginhh;
		String curbeginhh = preendhh;
		if(curenddate.endsWith(":00:00")) {
			//copyCssInfoToHive(jobctx);
		}
		
		HiveExecutor hiveExec = (HiveExecutor)cmap.get("hiveExec");
		DataFrame alldf = hiveExec.executeSQL("stat@selectStatCcuAll"
				, prebegindt, prebeginhh, preenddt, preendhh, prebegindate, preenddate
				, curbegindt, curbeginhh, curenddt, curendhh, curbegindate, curenddate
				, posbegindt, posbeginhh, posenddt, posendhh, posbegindate, posenddate
				);
		long collectT = System.currentTimeMillis();
		String statmin = curbegindate.replaceAll("[-: ]", "").substring(0,12);
		java.sql.Timestamp chgtime = new java.sql.Timestamp(collectT);
		Date chgdate = new Date(collectT);
		String esdate = new SimpleDateFormat(ES_FORMAT).format(chgdate);
		
		JavaPairRDD<String, Row> allrdd = alldf.javaRDD().mapToPair(new StatCcuJob.KeyPairFunc());
		JavaPairRDD<String, Iterable<Row>> listrdd = allrdd.groupByKey();
		JavaPairRDD<String, Ccu> ccurdd = listrdd.mapToPair(
				new KeyRowToCssCcuFunc(curbegindate, prewindow, window, poswindow));
		JavaPairRDD<String, Ccu> cssrdd = ccurdd.reduceByKey(new ReduceCssFunc());
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
		appdf.persist();
		long appCnt = 0;//appdf.count();
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
		sodf.persist();
		long soCnt = 0;//sodf.count();
		long soT = System.currentTimeMillis();
		HiveClient.saveTable(sotable, hivemode, hiveformat, sodf, "dt", "hh");
		long hiveT3 = System.currentTimeMillis();
		EsClient.saveES(sodf, sotable+"/docs");
		long esT3 = System.currentTimeMillis();
		
		cssdf.unpersist();
		appdf.unpersist();
		sodf.unpersist();
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
	
	public static class KeyRowToCssCcuFunc
			implements PairFunction<Tuple2<String, Iterable<Row>>, String, Ccu> {
		private int prewindow, window, poswindow;
		private Date beginDate;
		public KeyRowToCssCcuFunc(String begindate, int prewindow, int window, int poswindow) {
			this.prewindow = prewindow;
			this.window = window;
			this.poswindow = poswindow;
			try {
				this.beginDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(begindate);
			} catch(Exception e) {
				throw new RuntimeException(e);
			}
		}
		public Tuple2<String, Ccu> call(Tuple2<String, Iterable<Row>> rowpair) {
			String cssip = rowpair._1.split("_")[0];
			String appid = null;
			String soid = null;
			String flag = null;
			String motion = null;
			String preonTime = null;
			String posofTime = null;
			int sec = -1;
			int[] ccus = new int[window];
			for(Row row : rowpair._2) {
				if(appid==null) appid = row.getString(StatCcuJob._CCU_APP);
				if(soid==null) soid = row.getString(StatCcuJob._CCU_SO);
				flag = row.getString(StatCcuJob._CCU_FLAG);
				motion = row.getString(StatCcuJob._CCU_MOTION);
				if(flag.equals("cur")) {
					sec = Integer.parseInt(row.getString(_CCU_REGDATE).substring(17,19));
					ccus[sec] = motion.equals("ON") ? 1 : -1;
				}
				if(flag.equals("pre")) {
					preonTime = row.getString(StatCcuJob._CCU_REGDATE);
				}
				if(flag.equals("pos")) {
					posofTime = row.getString(StatCcuJob._CCU_REGDATE);
				}
			}
			calculate(ccus, beginDate, prewindow, window, poswindow, preonTime, posofTime);
			Ccu ccu = new Ccu(cssip, appid, soid, ccus, null, null);
			return new Tuple2<String, Ccu>(cssip, ccu);
		}
	}

}

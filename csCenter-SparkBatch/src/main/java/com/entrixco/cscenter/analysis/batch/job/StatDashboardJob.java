package com.entrixco.cscenter.analysis.batch.job;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.batch.job.StatCcuMultiJob.Ccu;
import com.entrixco.cscenter.analysis.batch.util.BatchConfig;
import com.entrixco.cscenter.analysis.batch.util.HiveExecutor;

import scala.Tuple2;

public class StatDashboardJob extends BatchJob implements Serializable {

	private static final Logger logger = LoggerFactory.getLogger(StatDashboardJob.class);

	public static class Ccu implements Serializable {
		public String appid;
		public String soid;
		public int[][] minutes;

		public Ccu(String app, String so, int[][] mins) {
			appid = app;
			soid = so;
			minutes = mins;
		}
	}

	public static final int _CCU_KEY = 0, _CCU_REGDATE = 1, _CCU_MOTION = 2, _CCU_OFFDATE = 3, _CCU_APP = 4,
			_CCU_SO = 5, _CCU_CSS = 6, _CCU_MIN = 7, _CCU_APP_KEY = 7, _CCU_SO_KEY = 8;
	public static final String[] SO_FIELDS = { "stat_min", "app_id", "so_id", "css_ccu", "avg_ccu", "min_ccu",
			"max_ccu", "chg_date", "dt", "hh" };
	public static final DataType[] SO_TYPES = { DataTypes.StringType, DataTypes.StringType, DataTypes.StringType,
			DataTypes.StringType, DataTypes.DoubleType, DataTypes.IntegerType, DataTypes.IntegerType,
			DataTypes.StringType, DataTypes.StringType, DataTypes.StringType };

	public void execute(JobExecutionContext jobctx) {
		long beginT = System.currentTimeMillis();
		Map<String, Object> cmap = getConfigMap(jobctx);
		int delay = Integer.parseInt(BatchConfig.getDefault(cmap, "stat_dash", "batch.delay"));
		int poswindow = Integer.parseInt(BatchConfig.getDefault(cmap, "stat_dash", "batch.poswindow"));
		int window = Integer.parseInt(BatchConfig.getDefault(cmap, "stat_dash", "batch.window"));
		int prewindow = Integer.parseInt(BatchConfig.getDefault(cmap, "stat_dash", "batch.prewindow"));
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
		String curenddt = dtForm.format(curend);
		String curendhh = hhForm.format(curend);

		String curbegindate = dateForm.format(curbegin);
		String curbegindt = dtForm.format(curbegin);

		String prebegindate = dateForm.format(prebegin);
		String prebegindt = dtForm.format(prebegin);
		String prebeginhh = hhForm.format(prebegin);

		String chgdate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+09:00").format(curtime);

		HiveExecutor hiveExec = (HiveExecutor) cmap.get("hiveExec");
		DataFrame alldf = hiveExec.executeSQL("stat@selectDashCcuSO", prebegindt, prebeginhh, curenddt, curendhh,
				prebegindate, curenddate, curbegindate);
		long collectT = System.currentTimeMillis();

		JavaPairRDD<String, Row> allrdd = alldf.javaRDD().mapToPair(new KeyPairFunc2());
		JavaPairRDD<String, Iterable<Row>> listrdd = allrdd.groupByKey();
		JavaPairRDD<String, Ccu> ccurdd = listrdd.mapToPair(new KeyRowToSoCcuFunc2(window, curbegin));
		JavaPairRDD<String, Ccu> sordd = ccurdd.reduceByKey(new ReduceCcuFunc2());
		JavaRDD<Row> sorowrdd = sordd.flatMap(new SoRowFunc2(curbegin, chgdate));

		HiveContext hiveCtx = (HiveContext) cmap.get("hiveCtx");
		StructField[] fields = new StructField[SO_FIELDS.length];
		for (int f = 0; f < SO_FIELDS.length; f++) {
			fields[f] = DataTypes.createStructField(SO_FIELDS[f], SO_TYPES[f], true);
		}
		DataFrame cssdf = hiveCtx.createDataFrame(sorowrdd, new StructType(fields));
		long cssCnt = cssdf.count();// cssdf.count();
		long cssT = System.currentTimeMillis();

//		DataFrame soccu = cssdf.groupBy("stat_min", "app_id", "so_id", "chg_date")
//				.agg(functions.avg("ccu").alias("ccu")).select("stat_min", "app_id", "so_id", "ccu", "chg_date");
//		long soccuT = System.currentTimeMillis();
//		long soccuCnt = soccu.count();

		// String ccutable = BatchConfig.getDefault(cmap, "stat_dash",
		// "ccu.table");
		// if(soccuCnt>0)
		// soccu.write().mode(SaveMode.Overwrite).saveAsTable(ccutable);
		if (cssCnt > 0) {
			StringBuilder csb = new StringBuilder();
			Row[] ccurow = cssdf.collect();

			for (Row r : ccurow) {
				csb.append(r.get(0) + "|" + r.get(1) + "|" + r.get(2) + "|" + r.get(4) + "|" + r.get(7) + "\n");
			}

			String clp = BatchConfig.getDefault(cmap, "stat_dash", "ccu.local.path");
			String chp = BatchConfig.getDefault(cmap, "stat_dash", "ccu.hive.path");
			uploadListToHadoop(csb, clp, chp);
		}
		long ccuhiveT = System.currentTimeMillis();

		DataFrame souvpv = executeCss(cmap, curenddate, curbegindate, curenddt, curbegindt, curendhh, curbegindt);
		long uvpv = System.currentTimeMillis();
		long uvpvCnt = souvpv.count();
		// String uvpvtable = BatchConfig.getDefault(cmap, "stat_dash",
		// "uvpv.table");
		// if(uvpvCnt>0)
		// souvpv.write().mode(SaveMode.Overwrite).saveAsTable(uvpvtable);
		if (uvpvCnt > 0) {
			StringBuilder usb = new StringBuilder();

			Row[] uvpvrow = souvpv.collect();

			for (Row r : uvpvrow) {
				usb.append(r.get(0) + "|" + r.get(1) + "|" + r.get(2) + "|" + r.get(3) + "|" + r.get(4) + "|" + r.get(5) + "\n");
			}

			String ulp = BatchConfig.getDefault(cmap, "stat_dash", "uvpv.local.path");
			String uhp = BatchConfig.getDefault(cmap, "stat_dash", "uvpv.hive.path");
			uploadListToHadoop(usb, ulp, uhp);
		}
		long uvpvhiveT = System.currentTimeMillis();

		long endT = System.currentTimeMillis();

		logger.info("##################################################");
		logger.info(
				"collect={}" + ", cssT({})={}" + ", ccuhiveT={}" + ", uvpvT({})={}, uvpvhiveT={}"
						+ ", total={}",
				(collectT - beginT) / 1000.0, cssCnt, (cssT - beginT) / 1000.0,
				ccuhiveT, uvpvCnt, (uvpv - beginT) / 1000.0, uvpvhiveT, (endT - beginT) / 1000.0);
		logger.info("##################################################");

		// executeDashboard2(cmap, curbegin);
	}

	public static class KeyPairFunc2 implements PairFunction<Row, String, Row> {
		public Tuple2<String, Row> call(Row row) {
			String key = row.getString(_CCU_SO_KEY);
			return new Tuple2<String, Row>(key, row);
		};
	}

	public static class KeyRowToSoCcuFunc2 implements PairFunction<Tuple2<String, Iterable<Row>>, String, Ccu> {
		private int window;
		private Date beginDate;

		public KeyRowToSoCcuFunc2(int window, Date begindate) {
			this.window = window;
			this.beginDate = begindate;
		}

		public Tuple2<String, Ccu> call(Tuple2<String, Iterable<Row>> rowpair) {
			long begintime = beginDate.getTime();
			long endtime = begintime + window * 1000;
			int mincnt = window / 60;
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			String appid = null, soid = null;
			long ontime = 0, offtime = 0;
			int onsec = 0, offsec = 0;
			int[][] mins = new int[mincnt][60];
			try {
				for (Row row : rowpair._2) {

					if (appid == null)
						appid = row.getString(_CCU_APP);
					if (soid == null)
						soid = row.getString(_CCU_SO);

					ontime = dateFormat.parse(row.getString(_CCU_REGDATE)).getTime();
					if (ontime < begintime)	mins[0][0] = 1;
					else {
						onsec = (int)(ontime-begintime)/1000;
						if(onsec/60<mincnt && onsec/60 >= 0){
							mins[onsec/60][onsec%60] = 1;
						}else{
							mins[0][0]=-1;
						}
					}
					offtime = dateFormat.parse(row.getString(_CCU_OFFDATE)).getTime();
					if (offtime >= endtime)
						;
					else {
						offsec = (int)(offtime-begintime)/1000;
						if(offsec/60<mincnt && offsec/60 >= 0){
							mins[offsec/60][offsec%60] = -1;
						}
					}
				}
			} catch (Exception e) {
				logger.error("KeyRowToSoCcuFunc2", e);
				throw new RuntimeException(e);
			}
			int pre = 0;
			for (int m = 0; m < mincnt; m++) {
				for (int s = 0; s < 60; s++) {
					switch (mins[m][s]) {
					case 1:
						pre = 1;
						break;
					case -1:
						mins[m][s] = 0;
						pre = 0;
						break;
					case 0:
						if (pre == 1)
							mins[m][s] = 1;
						break;
					}
				}
			}
			Ccu ccu = new Ccu(appid, soid, mins);
			return new Tuple2<String, Ccu>(soid, ccu);
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

	public static DataFrame executeCss(Map<String, Object> cmap, String curenddate, String curbegindate,
			String curenddt, String curbegindt, String curendhh, String curbeginhh) {
		HiveExecutor hiveExec = (HiveExecutor) cmap.get("hiveExec");
		DataFrame cssdf = hiveExec.executeSQL("stat@selectDashUpBandSo", curbegindt, curbeginhh, curenddt, curendhh,
				curbegindate, curenddate, curbegindt, curbeginhh, curenddt, curendhh, curbegindate, curenddate,
				curbegindt, curbeginhh, curenddt, curendhh, curbegindate, curenddate);

		return cssdf;
	}

	public static void uploadListToHadoop(StringBuilder sb, String lp, String hp) {
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
}

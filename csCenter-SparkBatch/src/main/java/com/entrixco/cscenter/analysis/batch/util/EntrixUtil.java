package com.entrixco.cscenter.analysis.batch.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntrixUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(EntrixUtil.class);
	
	public static void main(String[] args) {
		String filename = "";
		String type = "";
		for(int a=0; a<args.length; a++) {
			if(args[a].equals("--entrix_file") && args.length>a+1) {
				filename = args[a+1];
			}
		}
		if(filename.length()==0) {
			logger.info("Usage : Entrix --entrix_file filepath");
			return;
		}
		EntrixUtil.init(filename);
	}
	
	public static void test(String[] args) {
		//String specials = "|[](){}+*^$";
		String res1 = "[ INFO ] 2016-01-01 00:00:00.053 [000000_{08:eb:74:07:75:0f}_10.30.67.195]  -  Response Route Msg : ( 0, SUCCESS, {10.37.223.103}, 3390, 960, 540, 0, 0 )";
		String res2 = "[ INFO ] 2016-01-01 00:00:00.053 [10.37.229.101]  -  Response Status Msg : ( 0, SUCCESS )";
		String req1 = "[ INFO ] 2016-01-01 00:00:00.052 [10.37.232.102]  -  Request Status Msg : ( {10.37.232.102}, 7, 23200, OPEN, {08:eb:74:0f:16:ff}, ,  )";
		String req2 = "[ INFO ] 2016-01-01 00:00:00.051 [000000_{08:eb:74:07:75:0f}_10.30.67.195]  -  Request Route Msg : ( {08:eb:74:07:75:0f}, INIT, 23, 46 )";
		String stat1 = "[2016-01-01 00:00:02] registered_date=2016-01-01 00:00:02,css_session_id=19992,log_type=info,css_ip=10.37.224.22,menu_tree=21,motion=ON,motion_value=5,stb_id={08:eb:74:cd:9c:4b},so=54";
		java.util.regex.Pattern pat = java.util.regex.Pattern.compile(CSR);
		java.util.regex.Matcher mat = pat.matcher(res1);
		if(mat.matches()) {
			System.out.println("g1="+mat.group(1)+", g2="+mat.group(2)
					+", g3="+mat.group(3)+", g4="+mat.group(4)+", g5="+mat.group(5));
		}
		else {
			System.out.println("not matches");
		}
	}
	
	public static void init(String filename) {
		SparkConf spCnf = new SparkConf().setAppName("Entrix");
		SparkContext spCtx = new SparkContext(spCnf);
		JavaSparkContext jspCtx = new JavaSparkContext(spCtx);
		HiveContext hiveCtx = new HiveContext(spCtx);
		hiveCtx.sql("set hive.exec.dynamic.partition=true");
	    hiveCtx.sql("set hive.exec.dynamic.partition.mode=nonstrict");
	    
	    if(filename.substring(3).indexOf("_csrSvr.log")>0) {
	    	processCsr(jspCtx, hiveCtx, filename);
	    }
	    if(filename.substring(3).indexOf("_onoff.log")>0) {
	    	processStat(jspCtx, hiveCtx, filename);
	    }
	}
	
	public static final String CSR =
			"\\[.+?\\] ([\\d\\-]+? [\\d:\\.]+?) "
			+ "\\[(.+?)\\]\\s+?-\\s+?(Response|Request) "
			+ "(Route|Status) Msg : \\((.+?)\\)";
	public static final String[] CSR_HEAD = {
		"csr_id", "csr_date", "stb_mac", "stb_ip", "msg_mode",
		"msg_type", "css_ip", "app_id", "so", "msg"
	};
	public static void processCsr(JavaSparkContext jspCtx, HiveContext hiveCtx
			, String filepath) {
		int lastSlash = filepath.lastIndexOf('/');
		String filename = lastSlash<0 ? filepath : filepath.substring(lastSlash+1);
		final String cid = filename.substring(0, 3);
		JavaRDD<String> irdd = jspCtx.textFile(filepath);
		logger.info(">>>>>>>>>>>>>>>>>>> textFile readed : {}", filepath);
		JavaRDD<Row> trdd = irdd.map(new Function<String, Row>() {
			Pattern pattern = Pattern.compile(CSR);
			public Row call(String line) {
				//logger.info("> line : {}", line);
				Matcher matcher = pattern.matcher(line);
				if(!matcher.matches()) {
					//logger.info("> row not matched :");
					return RowFactory.create(new String[0]);
				}
				String cdate = matcher.group(1);
				String stbip = matcher.group(2);
				String[] stbs = stbip.split("_");
				String stbmac = "";
				String mode = matcher.group(3);
				String type = matcher.group(4);
				if(!type.equals("Route")) {
					//logger.info("> row is not Route :");
					return RowFactory.create(new String[0]);
				}
				String msg = matcher.group(5).trim();
				String[] msgs = msg.split(",");
				String cssip = "";
				String appid = "";
				String so = "";
				if(mode.equals("Request")) {
					stbmac = stbs[1].substring(1, stbs[1].length()-1);
					stbip = stbs[2];
					appid = msgs[2].trim();
					so = msgs[3].trim();
				}
				else if(mode.equals("Response")) {
					stbmac = stbs[1].substring(1, stbs[1].length()-1);
					stbip = stbs[2];
					cssip = msgs[2].trim();
					cssip = cssip.substring(1, cssip.length()-1);
				}
				String[] values = new String[] {
						cid, cdate, stbmac, stbip, mode, type, cssip, appid, so, msg
						};
				//logger.info("> row : {}", Arrays.toString(values));
				return RowFactory.create(values);
			}
		});
		logger.info(">>>>>>>>>>>>>>>>>>> row mapped : {}", "CSR");
		JavaRDD<Row> ordd = trdd.filter(new Function<Row, Boolean>() {
			public Boolean call(Row row) {return row.length()>0;}
		});
		logger.info(">>>>>>>>>>>>>>>>>>> row filtered : {}", "CSR");
		//if(ordd.count()==0) return;
		
		StructField[] sfs = new StructField[CSR_HEAD.length];
		for(int f=0; f<sfs.length; f++) {
			sfs[f] = DataTypes.createStructField(CSR_HEAD[f], DataTypes.StringType, true);
		}
		DataFrame df = hiveCtx.createDataFrame(ordd, new StructType(sfs));
		df.write().mode(SaveMode.Append).saveAsTable("default.tb_csr");
	}
	
	public static final String STAT =
			"\\[([\\d\\-]+? [\\d:\\.]+?)\\] (.+?)";
	public static final String[] STAT_HEAD = {
		"csr_id", "csr_date", "session_id", "log_type", "css_ip",
		"menu_tree", "motion", "motion_value", "stb_id", "so"
	};
	public static void processStat(JavaSparkContext jspCtx, HiveContext hiveCtx
			, String filepath) {
		int lastSlash = filepath.lastIndexOf('/');
		String filename = lastSlash<0 ? filepath : filepath.substring(lastSlash+1);
		final String cid = filename.substring(0, 3);
		JavaRDD<String> irdd = jspCtx.textFile(filepath);
		logger.info(">>>>>>>>>>>>>>>>>>> textFile readed : {}", filepath);
		JavaRDD<Row> trdd = irdd.map(new Function<String, Row>() {
			Pattern pattern = Pattern.compile(STAT);
			public Row call(String line) {
				Matcher matcher = pattern.matcher(line);
				if(!matcher.matches()) {
					return RowFactory.create(new String[0]);
				}
				String cdate = matcher.group(1);
				String[] msgs = matcher.group(2).trim().split(",");
				if(msgs.length!=9) {
					return RowFactory.create(new String[0]);
				}
				String sid = msgs[1].split("=")[1];
				String logtype = msgs[2].split("=")[1];
				String cssip = msgs[3].split("=")[1];
				String menutree = msgs[4].split("=")[1];
				String motion = msgs[5].split("=")[1];
				String motionval = msgs[6].split("=")[1];
				String stbid = msgs[7].split("=")[1];
				if(stbid.startsWith("{")) stbid = stbid.substring(1, stbid.length()-1);
				String so = msgs[8].split("=")[1];
				String[] values = new String[] {
						cid, cdate, sid, logtype, cssip, menutree, motion, motionval, stbid, so
						};
				//logger.info("row : {}", Arrays.toString(values));
				return RowFactory.create(values);
			}
		});
		logger.info(">>>>>>>>>>>>>>>>>>> row mapped : {}", "STAT");
		//JavaRDD<Row> ordd = trdd.filter(new Function<Row, Boolean>() {
		//	public Boolean call(Row row) {return row.length()>0;}
		//});
		//logger.info(">>>>>>>>>>>>>>>>>>> row filtered : {}", ordd.count());
		//if(ordd.count()==0) return;
		
		StructField[] sfs = new StructField[STAT_HEAD.length];
		for(int f=0; f<sfs.length; f++) {
			sfs[f] = DataTypes.createStructField(STAT_HEAD[f], DataTypes.StringType, true);
		}
		DataFrame df = hiveCtx.createDataFrame(trdd, new StructType(sfs));
		df.write().mode(SaveMode.Append).saveAsTable("default.tb_stat");
	}

}

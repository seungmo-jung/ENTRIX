package com.entrixco.cscenter.analysis.streaming.parser;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ApiParser2 extends StreamParser {

	private static final Logger logger = LoggerFactory.getLogger(ApiParser2.class);

	public static void parse(final HashMap<String, Object> configMap, JavaStreamingContext jstCtx) {
		
		final String debug=configMap.get("api.debug").toString();
		
		String topics = (String) configMap.get("apiTopic");
		String brokers = (String) configMap.get("brokers");
		String groupid = (String) configMap.get("sparkGroupID");
		CuratorFramework curatorFramework = (CuratorFramework) configMap.get("curatorFramework");

		JavaDStream<String> pairstream = createKafkaDStream(topics, brokers, groupid, jstCtx, curatorFramework);

		final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
		JavaDStream<String> trans = pairstream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
			public JavaRDD<String> call(JavaRDD<String> rdd) {
				OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
				offsetRanges.set(offsets);
				return rdd;
			}
		});

		JavaDStream<String> fil = trans.filter(new Function<String, Boolean>() {
			public Boolean call(String data) {
				JsonObject json = new JsonParser().parse(data).getAsJsonObject();
				String line = json.get("message").getAsString();
				String[] ar = line.split(",");
				String motion =ar[5].split("=")[1].trim();
				boolean mo = motion.equals("20") || motion.equals("SESSION_USE");
				return !mo;
			}
		});
		
		JavaDStream<Row> linestream	= fil.map(new Function<String, Row>() {
			public Row call(String t2) {
				String[] rt = null;
				try {
					JsonObject json = new JsonParser().parse(t2).getAsJsonObject();
					String line = json.get("message").getAsString();
					// String line = t2;
					String[] ar = line.split(",");
					for (int i = 0; i < ar.length; i++) {
						String[] data = ar[i].split("=");
						if (data.length > 1) {
							ar[i] = data[1].trim();
						} else {
							ar[i] = "";
						}
					}
					// stb_id {} 제거
					if(ar[7].startsWith("#")){
						ar[7] = ar[7].substring(1,ar[7].length());
					}
					ar[7] = subData(ar[7], "{");
					
					if( debug.equals("off") && ((ar[7].indexOf(":")<=0) || (ar[7].length()!=17)) ){
						return RowFactory.create(new String[0]);
					}
					String dt = ar[0].substring(0, 10).replace("-", "");
					String hh = ar[0].substring(11, 13);
					rt = new String[ar.length + 2];
					// rt[0] = "csr_id";
					System.arraycopy(ar, 0, rt, 0, ar.length);
					rt[ar.length] = dt;
					rt[ar.length + 1] = hh;
					if(!rt[5].equals("OFF")){
						rt[9]="";
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

				return RowFactory.create(rt);
			}
		});
		JavaDStream<Row> fapi = linestream.filter(new Function<Row, Boolean>() {
			public Boolean call(Row row) {
				return row.length()>0;
			}
		});
		
		String tablename = configMap.get("apiTable").toString();
		String[] columns = configMap.get("apiColumns").toString().split(",");
		HiveContext hiveCtx = (HiveContext) configMap.get("hiveCtx");

		// saveTable(linestream, tablename, columns, hiveCtx);
		saveTable(fapi, tablename, columns, hiveCtx, groupid, curatorFramework, offsetRanges);
	}

}

package com.entrixco.cscenter.analysis.streaming.parser;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
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

public class CsrWebParser extends StreamParser{
	
	private static final Logger logger = LoggerFactory.getLogger(CsrWebParser.class);

	public static void parse(final HashMap<String, Object> configMap, JavaStreamingContext jstCtx) {
		
		final SimpleDateFormat jdate = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss +0900");
		final SimpleDateFormat hdate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",Locale.KOREA);

		String topics = (String) configMap.get("csrwebTopic");
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

		JavaDStream<Row> linestream	= trans.map(new Function<String, Row>() {
			public Row call(String t2) {
				String[] rt = null;
//				logger.info(t2);
				try {
					JsonObject json = new JsonParser().parse(t2).getAsJsonObject();
					String line = json.get("message").getAsString();
					String csr_date = line.substring(line.indexOf("[")+1,line.indexOf("]"));
					Date jDate = jdate.parse(csr_date);
					csr_date = hdate.format(jDate);
					String host = json.get("host").getAsString().toUpperCase();
					String port = json.get("type").getAsString();
					if (port.indexOf(":")>0){
						port=port.split(":")[1].trim();
					}
					String dt = csr_date.substring(0, 10).replace("-", "");
					String hh = csr_date.substring(11, 13);
//					logger.info("{}, {}, {}, {}, {}",csr_date,host,port,dt,hh);
					rt = new String[] {csr_date,host,port,dt,hh};
				} catch (Exception e) {
					e.printStackTrace();
				}

				return RowFactory.create(rt);
			}
		});

		
		String tablename = configMap.get("csrwebTtable").toString();
		String[] columns = configMap.get("csrwebColumns").toString().split(",");
		HiveContext hiveCtx = (HiveContext) configMap.get("hiveCtx");

		saveTable(linestream, tablename, columns, hiveCtx, groupid, curatorFramework, offsetRanges);
	}

}

package com.entrixco.cscenter.analysis.streaming.parser;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class WebParser extends StreamParser {

	private static final Logger logger = LoggerFactory.getLogger(WebParser.class);
	private static final SimpleDateFormat jdate = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss +0900");
	private static final SimpleDateFormat hdate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",Locale.KOREA);

	private static HashMap<String, String> getColumns(String mapKeys) {
		HashMap<String, String> keyMap = new HashMap<>();
		String[] pairs = mapKeys.split(",");
		for (int i = 0; i < pairs.length; i++) {
			String pair = pairs[i];
			String[] keyValue = pair.split(":");
			if (keyValue.length == 2) {
				keyMap.put(keyValue[0], keyValue[1]);
			} else if (keyValue.length == 1) {
				keyMap.put(keyValue[0], null);
			}
		}
		return keyMap;
	}

	// @Override
	public static void parse(final HashMap<String, Object> configMap, JavaStreamingContext jstCtx) {

		String topics = configMap.get("webTopic").toString();
		String brokers = configMap.get("brokers").toString();
		String tablename = configMap.get("webTable").toString();
		String mapKeys = configMap.get("messageKeyMap").toString();
		String groupid = (String) configMap.get("sparkGroupID");
		CuratorFramework curatorFramework = (CuratorFramework) configMap.get("curatorFramework");

		final HashMap<String, String> columnMap = getColumns(mapKeys);// this.getColumns(mapKeys);
		final String[] columns = mapKeys.replaceAll(":(\\w+)|:()", "").split(",");

		JavaDStream<String> pairstream = createKafkaDStream(topics, brokers, groupid, jstCtx, curatorFramework);

		final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
		JavaDStream<String> trans = pairstream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
			public JavaRDD<String> call(JavaRDD<String> rdd) {
				OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
				offsetRanges.set(offsets);
				return rdd;
			}
		});

		JavaDStream<Row> linestream = trans.map(new Function<String, Row>() {
			public Row call(String t2) {
//				logger.info(t2);
				JsonObject jsonObject = new JsonParser().parse(t2).getAsJsonObject();
				int size = columns.length;
				String[] lines = new String[size];

				for (int index = 0; index < size - 2; index++) {

					String key = columnMap.get(columns[index]);

					if (key.equals("message")) {
						try {
							String line = jsonObject.get("message").getAsString();
							String web_date = line.substring(line.indexOf("[")+1,line.indexOf("]"));
							Date jDate = jdate.parse(web_date);
							web_date = hdate.format(jDate);
							lines[size - 2] = web_date.substring(0, 10).replace("-", "");
							lines[size - 1] = web_date.substring(11, 13);
							lines[index] = web_date;
						} catch (Exception e) {
							e.printStackTrace();
						}
					}else if(key.equals("host")){
						lines[index] = jsonObject.has(key) ? jsonObject.get(key).getAsString().toUpperCase() : "";
					}else {
						lines[index] = jsonObject.has(key) ? jsonObject.get(key).getAsString() : "";
					}
				}

				// logger.debug(">>>>> lines = {}",lines);
				return RowFactory.create(lines);
			}
		});

		HiveContext hiveCtx = (HiveContext) configMap.get("hiveCtx");
		// saveTable(linestream,tablename,columns,hiveCtx);
		saveTable(linestream, tablename, columns, hiveCtx, groupid, curatorFramework, offsetRanges);
	}
}

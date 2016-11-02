package com.entrixco.cscenter.analysis.streaming.parser;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import scala.Tuple2;

public class CsrParser extends StreamParser{
	
	private static final Logger logger = LoggerFactory.getLogger(CsrParser.class);
	
	public static final String CSR =
			"\\[.+?\\] ([\\d\\-]+? [\\d:\\.]+?) \\[(.+?)\\]\\s+?-\\s+?(Response|Request) "
			+ "(Route|Status) Msg : \\((.+?)\\)";
	
	public static void parse(final HashMap<String, Object> configMap,JavaStreamingContext jstCtx) {
		
		String topics = (String)configMap.get("csrTopic");
		String brokers = (String)configMap.get("brokers");
		String zookeeper = (String)configMap.get("zookeeper");
		String groupid = (String)configMap.get("sparkGroupID");
		int numThreads = Integer.parseInt((String)configMap.get("numThreads"));
//		JavaPairInputDStream<String, String> pairstream = createKafkaDStream(topics,brokers,jstCtx);
		JavaPairInputDStream<String, String> pairstream = createKafkaDStream(topics,brokers,groupid,jstCtx);
//		JavaPairInputDStream<String, String> pairstream = createKafkaStream(jstCtx,zookeeper,groupid,topics,numThreads);
		
		JavaPairDStream<String, String> rpds = typeFilter("Route",pairstream);
		JavaPairDStream<String, String> spds = typeFilter("Status",pairstream);
		
		JavaDStream<Row> rds = routeMap(rpds);
		JavaDStream<Row> sds = statusMap(spds);
		
		JavaDStream<Row> frds = lengthFilter(rds);
		JavaDStream<Row> fsds = lengthFilter(sds);
		
		String RouteTable = (String)configMap.get("RouteTable");
		String[] RouteColumns = configMap.get("RouteColumns").toString().split(",");

		String StatusTable = (String)configMap.get("StatusTable");
		String[] StatusColumns = configMap.get("StatusColumns").toString().split(",");
		
		HiveContext hiveCtx = (HiveContext)configMap.get("hiveCtx");
		
		saveTable(frds,RouteTable,RouteColumns,hiveCtx);
		saveTable(fsds,StatusTable,StatusColumns,hiveCtx);
	}
	

	
	public static JavaPairDStream<String, String> typeFilter(final String type,JavaPairInputDStream<String, String> pairstream){
		JavaPairDStream<String, String> rpds=pairstream.filter(new Function<Tuple2<String, String>, Boolean>() {
			Pattern pattern = Pattern.compile(CSR);
			public Boolean call(Tuple2<String, String> t2) {
				JsonObject json = new JsonParser().parse(t2._2()).getAsJsonObject();
				Matcher matcher = pattern.matcher(json.get("message").getAsString());
//				Matcher matcher = pattern.matcher(t2._2);
				if(!matcher.matches()) {
					return false;
				}
				return matcher.group(4).equals(type);
			}
		});
		return rpds;
	}
	
	public static JavaDStream<Row> routeMap(JavaPairDStream<String, String> pds){
		JavaDStream<Row> linestream = pds.map(new Function<Tuple2<String, String>, Row>() {
			Pattern pattern = Pattern.compile(CSR);
			public Row call(Tuple2<String, String> t2) {
				JsonObject json = new JsonParser().parse(t2._2()).getAsJsonObject();
				Matcher matcher = pattern.matcher(json.get("message").getAsString());
//				Matcher matcher = pattern.matcher(t2._2);
				if(!matcher.matches()) {
					return RowFactory.create(new String[0]);
				}
				String type = matcher.group(4);
				String[] stbs = matcher.group(2).split("_");
				String[] msgs = matcher.group(5).split(",");
				
				String cid = "csr_id";
				String cdate = matcher.group(1);
				String sessid =stbs[0];
				String stbmac = stbs[1];
				if(stbmac.startsWith("{")) stbmac=stbmac.substring(1, stbmac.length()-1);
				String stbip = stbs[2];
				String mode = matcher.group(3);
				String cssip = "";		//Response
				String appid = "";		//Request
				String so = "";			//Request
				String method = "";		//Request
				String result = "";		//Response
				String result_text = "";//Response
				String cssport = ""; 	//Response
				String width = ""; 		//Response
				String height = ""; 	//Response
				String va = ""; 		//Response
				String loading = ""; 	//Response
				String dt = cdate.substring(0,10).replace("-","");
				String hh = cdate.substring(11,13);
				
				if(type.equals("Route")) {
					if(mode.equals("Request")) {
						if(msgs.length<4){
							return RowFactory.create(new String[0]);
						}else{
							appid = msgs[2].trim();
							so = msgs[3].trim();
							method = msgs[1].trim();
						}
					}
					else if(mode.equals("Response")) {
						if(msgs.length<8){
							return RowFactory.create(new String[0]);
						}else{
							cssip = subData(msgs[2].trim(),"{");
//							if(cssip.startsWith("{")) cssip=cssip.substring(1, cssip.length()-1);
							result = msgs[0].trim();
							result_text = msgs[1].trim();
							cssport = msgs[3].trim();
							width = msgs[4].trim();
							height = msgs[5].trim();
							va = msgs[6].trim();
							loading = msgs[7].trim();
						}
					}
					String[] values = new String[] {
							cid,cdate,sessid,stbmac,stbip,mode,cssip,appid,so,method,result,result_text,cssport,width,height,va,loading,dt,hh
					};
					return RowFactory.create(values);
				}else{
					return RowFactory.create(new String[0]);
				}
			}
		});
		return linestream;
	}
	
	public static JavaDStream<Row> statusMap(JavaPairDStream<String, String> pds){
		JavaDStream<Row> linestream = pds.map(new Function<Tuple2<String, String>, Row>() {
			Pattern pattern = Pattern.compile(CSR);
			public Row call(Tuple2<String, String> t2) {
				JsonObject json = new JsonParser().parse(t2._2()).getAsJsonObject();
				Matcher matcher = pattern.matcher(json.get("message").getAsString());
//				Matcher matcher = pattern.matcher(t2._2);
				if(!matcher.matches()) {
					return RowFactory.create(new String[0]);
				}
				String type = matcher.group(4);
				String[] msgs = matcher.group(5).split(",");
				
				String cid = "csr_id";
				String cdate = matcher.group(1);
				String stbmac = "";			//Request
				String cssip = matcher.group(2);
				String mode = matcher.group(3);
				String users = "";			//Request
				String sessid = "";			//Request
				String sestatus = "";		//Request
				String result = "";			//Response
				String result_text = "";	//Response
				String dt = cdate.substring(0,10).replace("-","");
				String hh = cdate.substring(11,13);
				
				if(type.equals("Status")){
					if(mode.equals("Request")){
						if(msgs.length<6){
							return RowFactory.create(new String[0]);
						}else{
							stbmac = subData(msgs[4].trim(),"{");
//							if(stbmac.startsWith("{")) stbmac=stbmac.substring(1, stbmac.length()-1);
							users = msgs[1].trim();
							sessid = msgs[2].trim();
							sestatus = msgs[3].trim();
						}
					}else if(mode.equals("Response")){
						if(msgs.length<2){
							return RowFactory.create(new String[0]);
						}else{
							result = msgs[0].trim();
							result_text = msgs[1].trim();
						}
					}
					String[] values = new String[] {
							cid,cdate,stbmac,cssip,mode,users,sessid,sestatus,result,result_text,dt,hh
					};
					return RowFactory.create(values);
				}else{
					return RowFactory.create(new String[0]);
				}
			}
		});
		return linestream;
	}
	
	public static JavaDStream<Row> lengthFilter(JavaDStream<Row> ds){
		JavaDStream<Row> fds = ds.filter(new Function<Row, Boolean>() {
			public Boolean call(Row row) {
				return row.length()>0;
			}
		});
		return fds;
	}
}
//public JavaDStream<String> typeJsonFilter(final String type,JavaPairInputDStream<String, String> pairstream){
//JavaDStream<String> ds = pairstream.map(new Function<Tuple2<String, String>, String>() {
//	Pattern pattern = Pattern.compile(CSR);
//	public String call(Tuple2<String, String> t2){
//		JsonObject json = new JsonParser().parse(t2._2()).getAsJsonObject();
//		String line = json.get("message").getAsString();
//		Matcher matcher = pattern.matcher(line);
//		if(!matcher.matches()) {
//			return null;
//		}
//		
//		return null;
//	}
//});
//return ds;
//}


//	public JavaDStream<Row> dataMap(JavaPairDStream<String, String> pds){
//		JavaDStream<Row> linestream = pds.map(new Function<Tuple2<String, String>, Row>() {
//			Pattern pattern = Pattern.compile(CSR);
//			public Row call(Tuple2<String, String> t2) {
//				//데이터 처리//
//				Matcher matcher = pattern.matcher(t2._2());
//				if(!matcher.matches()) {
//					return RowFactory.create(new String[0]);
//				}
//				String cid = t2._1();
//				String cdate = matcher.group(1);
//				String dt = cdate.substring(0,10).replace("-","");
//				String hh = cdate.substring(11,13);
//				String[] stbs = matcher.group(2).split("_");
//				String stbmac = "";
//				String sessid ="";
//				String mode = matcher.group(3);
//				String type = matcher.group(4);
//				String msg = matcher.group(5).trim();
//				String[] msgs = msg.split(",");
//				String cssip = "";
//				String appid = "";
//				String so = "";
//				String method = "";
//				String result = "";
//				String result_text = "";
//				String cssport = "";
//				String width = "";
//				String height = "";
//				String va = "";
//				String loading = "";
//				String users = "";
//				String sestatus = "";
//				
//				if(type.equals("Route")) {
//					sessid = stbs[0];
//					stbmac = stbs[1];
//					if(stbmac.startsWith("{"))stbmac=stbmac.substring(1, stbs[1].length()-1);
//					String stbip = stbs[2];
//					if(mode.equals("Request")) {
//						if(msgs.length<4){
//							return RowFactory.create(new String[0]);
//						}else{
//							method = msgs[1].trim();
//							appid = msgs[2].trim();
//							so = msgs[3].trim();
//						}
//					}
//					else if(mode.equals("Response")) {
//						if(msgs.length<8){
//							return RowFactory.create(new String[0]);
//						}else{
//							cssip = msgs[2].trim();
//							if(cssip.startsWith("{")) cssip=cssip.substring(1, cssip.length()-1);
//							result = msgs[0].trim();
//							result_text = msgs[1].trim();
//							cssport = msgs[3].trim();
//							width = msgs[4].trim();
//							height = msgs[5].trim();
//							va = msgs[6].trim();
//							loading = msgs[7].trim();
//						}
//					}
//					String[] values = new String[] {
//							cid,cdate,sessid,stbmac,stbip,mode,cssip,appid,so,method,result,result_text,cssport,width,height,va,loading,dt,hh
//					};
//					return RowFactory.create(values);
//				}else if(type.equals("Status")){
//					if(mode.equals("Request")){
//						if(msgs.length<6){
//							return RowFactory.create(new String[0]);
//						}else{
//							cssip = stbs[0].trim();
//							stbmac = msgs[4].trim();
//							users = msgs[1].trim();
//							sessid = msgs[2].trim();
//							sestatus = msgs[3].trim();
//						}
//					}else if(mode.equals("Response")){
//						if(msgs.length<2){
//							return RowFactory.create(new String[0]);
//						}else{
//							result = msgs[0].trim();
//							result_text = msgs[1].trim();
//						}
//					}
//					String[] values = new String[] {
//							cid,cdate,stbmac,cssip,mode,users,sessid,sestatus,result,result_text,dt,hh
//					};
//					return RowFactory.create(values);
//				}else{
//					return RowFactory.create(new String[0]);
//				}
//			}
//		});
//		return linestream;
//	}

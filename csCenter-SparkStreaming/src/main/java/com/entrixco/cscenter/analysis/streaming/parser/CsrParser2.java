package com.entrixco.cscenter.analysis.streaming.parser;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class CsrParser2 extends StreamParser{
	
	private static final Logger logger = LoggerFactory.getLogger(CsrParser2.class);
	
	public static final String CSR =
			"\\[.+?\\] ([\\d\\-]+? [\\d:\\.]+?) \\[(.+?)\\]\\s+?-\\s+?(Response|Request) (Route|Status) Msg : \\((.+?)\\)";
//	"\\[.+?\\] ([\\d\\-]+? [\\d:\\.]+?) ([\\w\\-]+?), \\[(.+?)\\]\\s+?-\\s+?(Response|Request) (Route|Status) Msg : \\((.+?)\\)";
//			"\\[.+?\\] ([\\d\\-]+? [\\d:\\.]+?) \\[(.+?)\\]\\s+?-\\s+?(Response|Request) (Route|Status) Msg : \\((.+?)\\)";
	public static void parse(final HashMap<String, Object> configMap,JavaStreamingContext jstCtx) {
		String debug=configMap.get("csr.debug").toString();
		
		String topics = (String)configMap.get("csrTopic");
		String brokers = (String)configMap.get("brokers");
		String groupid = (String)configMap.get("sparkGroupID");
		
		CuratorFramework  curatorFramework = (CuratorFramework)configMap.get("curatorFramework");
		
		JavaDStream<String> pairstream = createKafkaDStream(topics, brokers, groupid, jstCtx, curatorFramework);
		
		final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
		JavaDStream<String> trans = pairstream.transform(new Function<JavaRDD<String>,JavaRDD<String>>(){
			public JavaRDD<String> call(JavaRDD<String> rdd){
				OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
				offsetRanges.set(offsets);
				return rdd;
			}
		}).persist();
		
		JavaDStream<String> rpds = typeFilter("Route",trans);
		JavaDStream<String> spds = typeFilter("Status",trans);
		
		JavaDStream<Row> rds = routeMap(rpds,debug);
		JavaDStream<Row> sds = statusMap(spds,debug);
		
		JavaDStream<Row> frds = lengthFilter(rds);
		JavaDStream<Row> fsds = lengthFilter(sds);
		
		String RouteTable = (String)configMap.get("RouteTable");
		String[] RouteColumns = configMap.get("RouteColumns").toString().split(",");
		
		String StatusTable = (String)configMap.get("StatusTable");
		String[] StatusColumns = configMap.get("StatusColumns").toString().split(",");
		
		HiveContext hiveCtx = (HiveContext)configMap.get("hiveCtx");
		
		saveTable(frds,RouteTable,RouteColumns,hiveCtx);
		saveTable(fsds,StatusTable,StatusColumns,hiveCtx,groupid,curatorFramework,offsetRanges);
		
//		saveTable(frds,RouteTable,RouteColumns,hiveCtx);
//		saveTable(fsds,StatusTable,StatusColumns,hiveCtx);
	}
	
	public static JavaDStream<String> typeFilter(final String type,JavaDStream<String> pairstream){
		JavaDStream<String> rpds=pairstream.filter(new Function<String, Boolean>() {
			Pattern pattern = Pattern.compile(CSR);
			public Boolean call(String t2) {
//				logger.info(t2);
				JsonObject json = new JsonParser().parse(t2).getAsJsonObject();
				Matcher matcher = pattern.matcher(json.get("message").getAsString());
//				Matcher matcher = pattern.matcher(t2);
				
				if(!matcher.matches()) {
					return false;
				}
				
				return matcher.group(4).equals(type);
			}
		});
		return rpds;
	}
	
	public static JavaDStream<Row> routeMap(JavaDStream<String> pds,final String debug){
		JavaDStream<Row> linestream = pds.map(new Function<String, Row>() {
			Pattern pattern = Pattern.compile(CSR);
			public Row call(String t2) {
				JsonObject json = new JsonParser().parse(t2).getAsJsonObject();
				String line = json.get("message").getAsString();
				String csr_port = json.has("type") ? json.get("type").getAsString() : "";
				if (csr_port.indexOf(":")>0){
					csr_port=csr_port.split(":")[1].trim();
				}
				Matcher matcher = pattern.matcher(line);
//				Matcher matcher = pattern.matcher(t2);
//				logger.info(">>>>route : {}",json.get("message").getAsString() );
				if(!matcher.matches()) {
					return RowFactory.create(new String[0]);
				}
				String type = matcher.group(4);
				String[] stbs = matcher.group(2).split("_");
				String[] msgs = matcher.group(5).split(",");
				
				String cid = json.get("host").getAsString().toUpperCase();
				
				String cdate = matcher.group(1);
				String sessid =stbs[0];
				String stbmac = stbs[1];
				if(stbmac.startsWith("#")){
					stbmac = stbmac.substring(1,stbmac.length());
				}
				stbmac=subData(stbmac, "{");
				String stbip = stbs[2];
				String stbport = "";
				if(stbip.indexOf("[")>0){
					String[] stbar = stbip.split("\\[");
					stbip = stbar[0];
					stbport = stbar[1].substring(0,stbar[1].length()-1);
				}
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
				
				
				if( debug.equals("off") && ( (stbmac.indexOf(":")<=0) || (stbmac.length()!=17) )){
					return RowFactory.create(new String[0]);
				}
				
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
							cid,csr_port,cdate,sessid,stbmac,stbip,stbport,mode,cssip,appid,so,method,result,result_text,cssport,width,height,va,loading,dt,hh
					};
					return RowFactory.create(values);
				}else{
					return RowFactory.create(new String[0]);
				}
			}
		});
		return linestream;
	}
	
	public static JavaDStream<Row> statusMap(JavaDStream<String> pds, final String debug){
		JavaDStream<Row> linestream = pds.map(new Function<String, Row>() {
			Pattern pattern = Pattern.compile(CSR);
			public Row call(String t2) {
				JsonObject json = new JsonParser().parse(t2).getAsJsonObject();
				String line = json.get("message").getAsString();
				Matcher matcher = pattern.matcher(line);
				String csr_port = json.has("type") ? json.get("type").getAsString() : "";
				if (csr_port.indexOf(":")>0){
					csr_port=csr_port.split(":")[1].trim();
				}
//				Matcher matcher = pattern.matcher(t2);
//				logger.info(">>>>status : {}",json.get("message").getAsString() );
				if(!matcher.matches()) {
					return RowFactory.create(new String[0]);
				}
				String type = matcher.group(4);
				String[] msgs = matcher.group(5).split(",");
				
				String csr_host = json.get("host").getAsString().toUpperCase();
				
				String cdate = matcher.group(1);
				String stbmac = "";			//Request
				String cssip = "";
				String csrse = "";
				String csr_ip_se = matcher.group(2);
				if(csr_ip_se.indexOf("[")>0){
					String[] cssar = csr_ip_se.split("\\[");
					csrse = cssar[1].substring(0,cssar[1].length()-1);
				}
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
							stbmac=msgs[4].trim();
							if(stbmac.startsWith("#")){
								stbmac = stbmac.substring(1,stbmac.length());
							}
							stbmac = subData(stbmac,"{");
							
							if( debug.equals("off") && ((stbmac.indexOf(":")<=0) || (stbmac.length()!=17) )){
								return RowFactory.create(new String[0]);
							}
							cssip = subData(msgs[0].trim(), "{");
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
//							sessid = msgs[2].trim();
//							stbmac = msgs[3].trim();
//							if(stbmac.startsWith("#")){
//								stbmac = stbmac.substring(1,stbmac.length());
//							}
//							stbmac = subData(stbmac,"{");
							if( debug.equals("off") && ((stbmac.indexOf(":")<=0) || (stbmac.length()!=17) )){
								return RowFactory.create(new String[0]);
							}
						}
					}
					String[] values = new String[] {
							csr_host,csr_port,cdate,stbmac,cssip,csrse,mode,users,sessid,sestatus,result,result_text,dt,hh
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

package com.entrixco.cscenter.analysis.batch.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;

import com.entrixco.cscenter.analysis.batch.job.StatCcuJob;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class BatchTest {
	
	public static void main(String[] args) {
		//testStatCCU(args);
		//testUTC(args);
		testJDBC(args);
		//testStream(args);
		//testDirectStream(args);
	}
	
	public static void testStream(String[] args) {
		if(args.length<3) {
			System.out.println("Usage : BatchTest zk group topic");
			return;
		}
		SparkConf spCnf = new SparkConf().setAppName("BatchTest");
		SparkContext spCtx = new SparkContext(spCnf);
		JavaSparkContext jspCtx = new JavaSparkContext(spCtx);
		JavaStreamingContext jstCtx = new JavaStreamingContext(jspCtx, Durations.seconds(10));
		jstCtx.addStreamingListener(new StreamingListener() {
			public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				System.out.println("batch completed : records="+batchCompleted.batchInfo().numRecords());
				System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
			}
			public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {}
			public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {}
			public void onReceiverError(StreamingListenerReceiverError receiverError) {}
			public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {}
			public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {}
		});
		String zkQuorum = args[0];//ent-dev-agc02:2181,...;
		String groupId = args[1];
		HashMap<String, Integer> topicMap = new HashMap<>(); topicMap.put(args[2], 1);
		JavaPairInputDStream<String, String> pairstream = KafkaUtils.createStream(
				jstCtx, zkQuorum, groupId, topicMap);
		pairstream.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
			public Void call(JavaPairRDD<String, String> rdd) {
				rdd.foreach(new VoidFunction<Tuple2<String, String>>() {
					public void call(Tuple2<String, String> tuple2) {
						System.out.println("###########################["+tuple2._1+"] : ["+tuple2._2+"]");
					}
				});
				return null;
			}
		});
		jstCtx.start();
		jstCtx.awaitTermination();
	}
	
	public static void testDirectStream(String[] args) {
		if(args.length<3) {
			System.out.println("Usage : BatchTest zk group topic boot");
			return;
		}
		SparkConf spCnf = new SparkConf().setAppName("BatchTest");
		SparkContext spCtx = new SparkContext(spCnf);
		JavaSparkContext jspCtx = new JavaSparkContext(spCtx);
		JavaStreamingContext jstCtx = new JavaStreamingContext(jspCtx, Durations.seconds(10));
		jstCtx.addStreamingListener(new StreamingListener() {
			public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
				System.out.println("#######################################");
				System.out.println("batch completed : records="+batchCompleted.batchInfo().numRecords()
						+", time="+new Date(batchCompleted.batchInfo().submissionTime()).toString()
						+", schedule="+batchCompleted.batchInfo().schedulingDelay().toString()
						+", processing="+batchCompleted.batchInfo().processingDelay().toString());
				System.out.println("#######################################");
			}
			public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {}
			public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {}
			public void onReceiverError(StreamingListenerReceiverError receiverError) {}
			public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {}
			public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {}
		});
		final String zkQuorum = args[0];//ent-dev-agc02:2181,...;
		final String groupId = args[1];
		HashSet<String> topicSet = new HashSet<>(Arrays.asList(args[2].split(",")));
		final String bootServers = args[3];
		System.out.println("######################## "
				+"zk=["+zkQuorum+"], grp=["+groupId
				+"], topic=["+args[2]+"], boot=["+bootServers+"]");
		final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
		final CuratorFramework curator = CuratorFrameworkFactory.builder()
				.namespace("consumers").connectString(zkQuorum)
				.connectionTimeoutMs(1000).sessionTimeoutMs(10000)
				.retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
		curator.start();
		try {
			int partitions = 3;
			for(String topic : topicSet){
				for(int p=0; p<partitions; p++){
					String nodePath = "/"+groupId+"/offsets/"+topic+"/"+p;
					byte[] offset = "0".getBytes();
					if(curator.checkExists().forPath(nodePath)!=null){
						offset = curator.getData().forPath(nodePath);						
					}
					System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>> Start ["
							+nodePath+"] offset="+(new String(offset)));
				}
			}
		} catch (Exception e) {
			jstCtx.close();
			throw new RuntimeException(e);
		}
		
		HashMap<String, String> kafkaParams= new HashMap<String, String>();
		//kafkaParams.put("metadata.broker.list", bootServers);
		kafkaParams.put("bootstrap.servers", bootServers);
		JavaPairInputDStream<String, String> pairstream = KafkaUtils.createDirectStream(
				jstCtx, String.class, String.class, StringDecoder.class, StringDecoder.class,
				kafkaParams, topicSet);
		JavaPairDStream<String, String> pairstream1 = pairstream.transformToPair(
			new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
				public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
					OffsetRange[] offsets = ((HasOffsetRanges)rdd.rdd()).offsetRanges();
					offsetRanges.set(offsets);
					return rdd;
				}
			}
		);
		JavaDStream<String> linestream = pairstream1.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> tu2) {
				return "line : ["+tu2._1+" : "+tu2._2+"]";
			}
		});
		
		linestream.foreachRDD(new Function<JavaRDD<String>, Void>() {
			public Void call(JavaRDD<String> rdd) throws Exception {
				//rdd.foreach(new VoidFunction<String>() {
				//	public void call(String line) {System.out.println(line);}
				//});
				
				ObjectMapper omapper = new ObjectMapper();
				for(OffsetRange o : offsetRanges.get()) {
					byte[] offsetBytes = omapper.writeValueAsBytes(o.untilOffset());//o.fromOffset());
					String nodePath = "/"+groupId+"/offsets/"+o.topic()+"/"+o.partition();
					System.out.println(">>>>>>>>>>>>>>>>>>>>>>>> Current ["+nodePath
							+"] from="+o.fromOffset()+", until="+o.untilOffset());
					if(curator.checkExists().forPath(nodePath)!=null)
						curator.setData().forPath(nodePath,offsetBytes);
					else
						curator.create().creatingParentsIfNeeded().forPath(nodePath, offsetBytes);
				}
				return null;
			}
		});
		jstCtx.start();
		jstCtx.awaitTermination();
	}
	
	private static JavaStreamingContext directContext(String checkpointDir
			, String bootServers, String zkQuorum, HashSet<String> topicSet
			, final String groupId) {
		final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
		final CuratorFramework curator = CuratorFrameworkFactory.builder()
				.namespace("consumers").connectString(zkQuorum)
				.connectionTimeoutMs(1000).sessionTimeoutMs(10000)
				.retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
		curator.start();
		
		SparkConf spCnf = new SparkConf().setAppName("BatchTest");
		SparkContext spCtx = new SparkContext(spCnf);
		JavaSparkContext jspCtx = new JavaSparkContext(spCtx);
		JavaStreamingContext jstCtx = new JavaStreamingContext(jspCtx, Durations.seconds(10));
		jstCtx.checkpoint(checkpointDir);
		
		HashMap<String, String> kafkaParams= new HashMap<String, String>();
		//kafkaParams.put("metadata.broker.list", bootServers);
		kafkaParams.put("bootstrap.servers", bootServers);
		JavaPairInputDStream<String, String> pairstream = KafkaUtils.createDirectStream(
				jstCtx, String.class, String.class, StringDecoder.class, StringDecoder.class,
				kafkaParams, topicSet);
		JavaPairDStream<String, String> pairstream1 = pairstream.transformToPair(
			new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
				public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
					OffsetRange[] offsets = ((HasOffsetRanges)rdd.rdd()).offsetRanges();
					offsetRanges.set(offsets);
					return rdd;
				}
			}
		);
		pairstream1.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
			public Void call(JavaPairRDD<String, String> rdd) throws Exception {
				ObjectMapper omapper = new ObjectMapper();
				for(OffsetRange o : offsetRanges.get()) {
					byte[] offsetBytes = omapper.writeValueAsBytes(o.untilOffset());//o.fromOffset());
					String nodePath = "/"+groupId+"/offsets/"+o.topic()+"/"+o.partition();
					System.out.println(">>>>>>>>>>>>>>>>>>>>>>>> Current ["+nodePath
							+"] from="+o.fromOffset()+", until="+o.untilOffset());
					if(curator.checkExists().forPath(nodePath)!=null)
						curator.setData().forPath(nodePath,offsetBytes);
					else
						curator.create().creatingParentsIfNeeded().forPath(nodePath, offsetBytes);
				}
				return null;
			}
		});
		return jstCtx;
	}
	public static void testCheckpoint(String[] args) {
		if(args.length<3) {
			System.out.println("Usage : BatchTest zk group topic boot");
			return;
		}
		final String checkpointDir = "";
		final String zkQuorum = args[0];//ent-dev-agc02:2181,...;
		final String groupId = args[1];
		final HashSet<String> topicSet = new HashSet<>(Arrays.asList(args[2].split(",")));
		final String bootServers = args[3];
		System.out.println("######################## "
				+"zk=["+zkQuorum+"], grp=["+groupId
				+"], topic=["+args[2]+"], boot=["+bootServers+"]");
		
		JavaStreamingContextFactory contextFactory = new JavaStreamingContextFactory() {
			@Override public JavaStreamingContext create() {
				return directContext(checkpointDir, bootServers, zkQuorum, topicSet, groupId);
			}
		};
		JavaStreamingContext jstCtx = JavaStreamingContext.getOrCreate(checkpointDir, contextFactory);
		
		jstCtx.addStreamingListener(new StreamingListener() {
			public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
				System.out.println("#######################################");
				System.out.println("batch completed : records="+batchCompleted.batchInfo().numRecords()
						+", time="+new Date(batchCompleted.batchInfo().submissionTime()).toString()
						+", schedule="+batchCompleted.batchInfo().schedulingDelay().toString()
						+", processing="+batchCompleted.batchInfo().processingDelay().toString());
				System.out.println("#######################################");
			}
			public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {}
			public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {}
			public void onReceiverError(StreamingListenerReceiverError receiverError) {}
			public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {}
			public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {}
		});
		jstCtx.start();
		jstCtx.awaitTermination();
	}
	
	public static void testJDBC(String[] args) {
		String driver = "org.apache.hive.jdbc.HiveDriver";
		//String url = "jdbc:hive2://192.168.7.204:10000,192.168.7.205:10000";
		String url = "jdbc:hive2://ent-dev-agg01:2181,ent-dev-agg02:2181,ent-dev-agg03:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
		String user = "yarn";
		String passwd = "";
		Connection conn = null;
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, passwd);
			ResultSet rset = conn.createStatement().executeQuery("select * from tb_cssinfo");
			while(rset.next()) {
				//System.out.println("so_id="+rset.getString("so_id")
				//	+", app_id="+rset.getString("app_id")+", css_ip="+rset.getString("css_ip"));
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(conn!=null) {
				try {conn.close();} catch(Exception fe) {}
			}
		}
	}
	
	public static void testUTC(String[] args) {
		String in = "2016-04-26T07:31:19.128Z";
		DateFormat df = DateFormat.getDateInstance();
		SimpleDateFormat zdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		zdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		try {
			Date d = zdf.parse(in);
			String out = sdf.format(d);
			System.out.println("in=["+in+"] out=["+out+"]");
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static void testStatCCU(String[] args) {
		int[] seconds = {
				0, -1, 0, 1, 1,
				0, -1, 0, -1, 0
		};
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		Date beginDate = null;
		try {
			beginDate = sdf.parse("20160101120000");
		} catch(Exception e) {throw new RuntimeException(e);}
		int preWindow = 30;
		int window = seconds.length;
		int posWindow = 30;
		String preOn = "2016-01-01 11:59:31";
		String posOff = "2016-01-01 12:00:39";
		StatCcuJob.calculate(seconds, beginDate, preWindow, window, posWindow, preOn, posOff);
		System.out.println(Arrays.toString(seconds));
	}

}

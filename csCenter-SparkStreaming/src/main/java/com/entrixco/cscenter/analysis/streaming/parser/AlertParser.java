package com.entrixco.cscenter.analysis.streaming.parser;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import kafka.Kafka;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaTestUtils;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.format.DateTimeFormatter;
import scala.Tuple2;

import java.util.*;

/**
 * Created by jonghyeonkim on 2016. 6. 13..
 */
public class AlertParser extends StreamParser {


    private static final Logger logger = LoggerFactory.getLogger(AlertParser.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
    private static final DateTimeFormatter pFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");


    //private static Producer<String, String> producer;
    private static Producer<String, String> producer;

    //@Override
    public static void parse(final HashMap<String, Object> configMap, JavaStreamingContext jstCtx) {

        String topics       = configMap.get("alertTopic").toString();
        String brokers      = configMap.get("brokers").toString();
        final String outputTopic = configMap.get("alertOutTopic").toString();
        final String alertKey   = configMap.get("alertKey").toString();
        final String alertValue = configMap.get("alertValue").toString();


        Properties props = new Properties();
        props.put("metadata.broker.list", brokers);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        producer = new Producer<String, String>(new ProducerConfig(props));

        JavaPairInputDStream<String, String> pairstream = createKafkaDStream(topics,brokers,jstCtx);
        pairstream.filter(new Function<Tuple2<String, String>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {

                    JsonObject jsonObject = new JsonParser().parse(stringStringTuple2._2).getAsJsonObject();

                    Boolean alertCheck = false;

                    if( jsonObject.get(alertKey).getAsString().equals(alertValue)){
                        alertCheck = true;
                    }else{
                        alertCheck = false;
                    }
                    return alertCheck;
                }
            }
        ).map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._2();
            }
        }).foreachRDD(new Function<JavaRDD<String>, Void>() {
            @Override
            public Void call(JavaRDD<String> stringJavaRDD) throws Exception {
                List <String> rddList = stringJavaRDD.collect();

                List<KeyedMessage<String, String>> messages = new ArrayList<KeyedMessage<String, String>>();
                for( String row : rddList ){
                    messages.add(new KeyedMessage<String, String>(outputTopic,row));
                }
                producer.send(messages);

                return null;
            }
        });
    }

}

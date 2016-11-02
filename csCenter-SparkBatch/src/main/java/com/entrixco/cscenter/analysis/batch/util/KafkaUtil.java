package com.entrixco.cscenter.analysis.batch.util;

import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaUtil {
	
	public static void main(String[] args) {
		if(args.length>0 && args[0].equals("--produce")) {
			produce(args);
		}
		//else if(args.length>0 && args[0].equals("--consume")) {
		//	consume(args);
		//}
		else System.out.println("Usage : java KafkaUtil --produce");
	}
	
	public static void produce(String[] args) {
		Properties props = new Properties();
		props.put("metadata.broker.list", "ent-eul-agg01:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		
		Producer<String, String> producer =
				new Producer<String, String>(new ProducerConfig(props));
		KeyedMessage<String, String> keymsg = new KeyedMessage<>("topic1", "key1", "value1");
		producer.send(keymsg);
		producer.close();
	}
	
	public static void consume(String[] args) {
		Properties props = new Properties();
		props.put("group.id", "group1");
		props.put("zookeeper.connect", "ent-eul-agg01:2181");
		props.put("auto.commit.interval.ms", "1000");
		
		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
	}

}

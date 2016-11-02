package com.entrixco.cscenter.analysis.batch.client;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaClient {
	
	public static void main(String[] args) {
		if(args.length>0 && args[0].equals("--produce")) {
			produce(args);
		}
		else if(args.length>0 && args[0].equals("--consume")) {
			consume(args);
		}
		else System.out.println("Usage : java KafkaClient --produce|--consume server topic [key] [value]");
	}
	
	public static void produce(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", args[1]);//"ent-eul-agg01:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String, String> producer = new KafkaProducer<>(props);
		ProducerRecord<String, String> record =
				new ProducerRecord<>(args[2], args[3], args[4]);//"topic1", "key1", "value1");
		producer.send(record);
		producer.close();
		System.out.printf("produce key=[%s], value=[%s]\n", "key1", "value1");
	}
	
	public static void consume(String[] args) {
		 Properties props = new Properties();
	     props.put("bootstrap.servers", args[1]);//"ent-eul-agg01:9092");
	     props.put("group.id", "group1");
	     props.put("enable.auto.commit", "true");
	     props.put("auto.commit.interval.ms", "1000");
	     props.put("session.timeout.ms", "30000");
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     
	     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
	     consumer.subscribe(Arrays.asList(args[2]));//"topic1"));
	     while (true) {
	         ConsumerRecords<String, String> records = consumer.poll(100);
	         for (ConsumerRecord<String, String> record : records)
	             System.out.printf("offset=%d, key=[%s], value=[%s]\n",
	            		 record.offset(), record.key(), record.value());
	     }
	}

}

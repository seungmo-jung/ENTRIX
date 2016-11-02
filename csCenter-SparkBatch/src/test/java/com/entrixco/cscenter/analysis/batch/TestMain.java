package com.entrixco.cscenter.analysis.batch;

import java.util.Properties;

public class TestMain {
	
	public static void main(String[] args) {
		Properties sysprops = System.getProperties();
		for(String key : sysprops.stringPropertyNames()) {
			System.out.println("["+key+"] : ["+sysprops.getProperty(key)+"]");
		}
		for(String arg : args) {
			System.out.println("{"+arg+"}");
		}
	}
	
	public static void testKafka(String[] args) {
		
	}

}

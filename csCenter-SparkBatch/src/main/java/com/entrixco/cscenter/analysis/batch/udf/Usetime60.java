package com.entrixco.cscenter.analysis.batch.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Usetime60 extends UDAF {
	
	private static final Logger logger = LoggerFactory.getLogger(Usetime60.class);
	
	public static class Usetime60Evaluator implements UDAFEvaluator {
		
		private long count;
		
		public Usetime60Evaluator() {
			super();
			init();
		}
		
		public void init() {
			count = 0;
		}
		
		public boolean iterate(long usetime) {
			if(usetime>50 && usetime<=60) count++;
			return true;
		}
		
		public long terminatePartial() {
			return count;
		}
		
		public boolean merge(long othercount) {
			count += othercount;
			return true;
		}
		
		public long terminate() {
			return count;
		}
	}
}

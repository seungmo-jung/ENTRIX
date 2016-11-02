package com.entrixco.cscenter.analysis.batch.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Usetime10 extends UDAF {
	
	private static final Logger logger = LoggerFactory.getLogger(Usetime10.class);
	
	public static class Usetime10Evaluator implements UDAFEvaluator {
		
		private Long count;
		
		public Usetime10Evaluator() {
			super();
			init();
		}
		
		public void init() {
			count = 0L;
		}
		
		public boolean iterate(Long usetime) {
			if(usetime>0 && usetime<=10) count++;
			return true;
		}
		
		public Long terminatePartial() {
			return count;
		}
		
		public boolean merge(Long othercount) {
			count += othercount;
			return true;
		}
		
		public Long terminate() {
			return count;
		}
	}

}

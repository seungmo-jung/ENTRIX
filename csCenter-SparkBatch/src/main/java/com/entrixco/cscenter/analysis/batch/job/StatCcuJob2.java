package com.entrixco.cscenter.analysis.batch.job;

import java.util.Map;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.batch.util.HiveExecutor;

public class StatCcuJob2 extends StatCcuJob {
	
	private static final Logger logger = LoggerFactory.getLogger(StatCcuJob2.class);
	
	@Override
	protected Map<String, Row> getPostMinimum(HiveExecutor hiveExec
			, String posbegindt, String posbeginhh, String posenddt
			, String posendhh, String posbegindate, String posenddate) {
		DataFrame posdf = hiveExec.executeSQL("stat@selectStatCcuByLagOff"
				, posbegindt, posbeginhh, posenddt, posendhh, posbegindate, posenddate);
		return posdf.javaRDD().mapToPair(new KeyPairFunc()).collectAsMap();
	}
	@Override
	protected Map<String, Row> getPrevMaximum(HiveExecutor hiveExec
			, String prebegindt, String prebeginhh, String preenddt
			, String preendhh, String prebegindate, String preenddate) {
		DataFrame predf = hiveExec.executeSQL("stat@selectStatCcuByLeadOn"
				, prebegindt, prebeginhh, preenddt, preendhh, prebegindate, preenddate);
		return predf.javaRDD().mapToPair(new KeyPairFunc()).collectAsMap();
	}

}

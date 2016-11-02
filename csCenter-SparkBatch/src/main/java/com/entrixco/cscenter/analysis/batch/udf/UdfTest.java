package com.entrixco.cscenter.analysis.batch.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class UdfTest extends UDF {
	
	public Text evaluate(final Text itext) {
		return new Text("output");
	}

}

package com.entrixco.cscenter.analysis.batch.client;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;

public class HiveClient {
	
	public static void saveTable(String table
			, String mode, String format, DataFrame df, String...partnames) {
		SaveMode savemode =
				mode!=null && mode.equals("overwrite") ? SaveMode.Overwrite : SaveMode.Append;
		
		if(format!=null && format.trim().length()>0)  {
			df.write().format(format).mode(savemode).partitionBy(partnames).saveAsTable(table);
		}
		else df.write().mode(savemode).partitionBy(partnames).saveAsTable(table);
	}

}

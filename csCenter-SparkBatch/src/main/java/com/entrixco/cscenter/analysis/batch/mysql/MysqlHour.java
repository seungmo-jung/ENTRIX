package com.entrixco.cscenter.analysis.batch.mysql;

import java.util.HashMap;

public abstract class MysqlHour {
	public abstract void saveMysql(String dt,String hh, HashMap<String, Object> configMap);
}

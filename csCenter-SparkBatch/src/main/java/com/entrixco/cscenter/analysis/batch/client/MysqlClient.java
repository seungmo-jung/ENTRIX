package com.entrixco.cscenter.analysis.batch.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.batch.util.BatchConfig;

public class MysqlClient {
	
	private static final Logger logger = LoggerFactory.getLogger(MysqlClient.class);
	
	public static void saveMysql(DataFrame df
			, String url, String user, String password, String table) {
		Properties props = new Properties();
		props.put("user", user);
		props.put("password", password);
		df.write().mode(SaveMode.Append).jdbc(url, table, props);
	}
	
	public static DataFrame readMysql(SQLContext sqlCtx
			, String url, String user, String password, String table) {
		DataFrame df = sqlCtx.read().format("jdbc")
				.option("driver", "com.mysql.jdbc.Driver")
				.option("url", url)
				.option("user", user)
				.option("password", password)
				.option("dbtable", table)
				.load();
		return df;
	}
	
	public static class MysqlData {
		public final String[] names;
		public final String[] types;
		public final List<Object[]> list;
		public final Map<String, Integer> index;
		public MysqlData(String[] names, String[] types, List<Object[]> list) {
			this.names = names;
			this.types = types;
			this.list = list;
			this.index = new HashMap<>();
			for(int i=0; i<names.length; i++) index.put(names[i], i);
		}
	}
	
	private Map<String, String> sqlMap;
	private String url;
	private String user;
	private String password;
	private String validation;
	private Connection myconn;
	
	public MysqlClient(Map<String, Object> cmap) {
		this.sqlMap = (Map<String, String>)cmap.get("sqlMap");
		this.url = (String)cmap.get("default.mysql.url");
		this.user = (String)cmap.get("default.mysql.user");
		this.password = (String)cmap.get("default.mysql.password");
		this.validation = (String)cmap.get("default.mysql.validation");
	}
	
	public void open() {
		if(myconn!=null) return;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			myconn = DriverManager.getConnection(url, user, password);
		} catch(Exception e) {
			logger.error("mysqlClient open", e);
			throw new RuntimeException(e);
		}
	}
	
	public void close() {
		if(myconn!=null) {
			try {myconn.close();} catch(Exception e) {}
		}
	}
	
	public MysqlData select(String sid, Object...params) {
		PreparedStatement pstmt = null;
		ResultSet rset = null;
		try {
			try {
				pstmt = myconn.prepareStatement(validation);
				pstmt.executeQuery();
			} catch(SQLException se) {
			} finally {
				if(pstmt!=null) {
					try {pstmt.close();} catch(SQLException se) {}
				}
			}
			//replaced for mysql : String sql = String.format(sqlMap.get(sid), params);
			String sql = sqlMap.get(sid);
			pstmt = myconn.prepareStatement(sql);
			for(int p=0; p<params.length; p++) pstmt.setObject(p+1, params);
			rset = pstmt.executeQuery();
			
			ResultSetMetaData meta = rset.getMetaData();
			String[] colnames = new String[meta.getColumnCount()];
			String[] coltypes = new String[colnames.length];
			for(int c=0; c<colnames.length; c++) {
				colnames[c] = meta.getColumnName(c+1);
				coltypes[c] = meta.getColumnTypeName(c+1);
			}
			List<Object[]> rowlist = new ArrayList<Object[]>();
			while(rset.next()) {
				Object[] row = new Object[colnames.length];
				for(int c=0; c<row.length; c++) row[c] = rset.getObject(c+1);
				rowlist.add(row);
			}
			return new MysqlData(colnames, coltypes, rowlist);
		} catch(Exception e) {
			logger.error("mysqlClient select", e);
			throw new RuntimeException(e);
		} finally {
			if(rset!=null) {
				try {rset.close();} catch(Exception e) {}
			}
			if(pstmt!=null) {
				try {pstmt.close();} catch(Exception e) {}
			}
		}
	}

}

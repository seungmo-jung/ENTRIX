package com.entrixco.cscenter.analysis.batch.backup;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.batch.util.BatchConfig;

public class BackupMain {
	private static final Logger logger = LoggerFactory.getLogger(BackupMain.class);
	
	static String rootPath;
	
	public static void main(String[] args) {
		HashMap<String, String> argMap = BatchConfig.readArgs(args);
		String configPath = "configs/backup_config.properties";
		if(argMap.containsKey("csconfig")) configPath = argMap.get("csconfig");
		HashMap<String, Object> configMap = BatchConfig.readConfig(configPath);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar cal = Calendar.getInstance();
		
		String[] type = {"css", "so", "app"}; 
//		String[] css = {"bandwidth"};
		String[] css = {"bandwidth", "ccu", "uvpv", "route", "status", "sed", "web"};
		String[] colTps = {"cpu", "disk", "mem", "network", "uplink", "csr_tps", "so_csrtps", "web_tps", "so_webtps"};
		
		rootPath = configMap.get("path").toString();
		
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			String hurl = configMap.get("hive.url").toString();
			String huser = configMap.get("hive.user").toString();
			String hpwd = configMap.get("hive.password").toString();
			Connection hconn = DriverManager.getConnection(hurl,huser,hpwd);
			Statement hsm = hconn.createStatement();
			ResultSet hrs;
			
			if(configMap.get("debug").equals("on")){
				cal.setTime(sdf.parse(configMap.get("date").toString()));
				cal.add(Calendar.DATE, 1);
			}
			
			int day = Integer.parseInt(configMap.get("day").toString());
			String dt;
			logger.info("day: {}", day);
			for(int i=0;i<day;i++){
				long dayS = System.currentTimeMillis();
				cal.add(Calendar.DATE, -1);
				dt = sdf.format(new Date(cal.getTimeInMillis()));
				logger.info("dt: {}",dt);
				String sql;
				for(String tb : css){
					for(String t : type){
						for(int j=0;j<24;j++){
							String hh="";
							if (j<10){
								hh="0"+j;
							}else{
								hh=""+j;
							}
							String table = "est_"+t+"_"+tb+"_min";
							sql = "SELECT * FROM "+table+" WHERE dt='"+dt+"' and hh='"+hh+"'";
							logger.info("select hive table: {}",sql);
							long queryS = System.currentTimeMillis();
							hrs = hsm.executeQuery(sql);
							long queryE = System.currentTimeMillis();
							int cnt = write(hrs,table,dt,hh);
							long writeT = System.currentTimeMillis();
							logger.info("###############################################");
							logger.info("{} queryT: {}, writeT: {}, count: {}", table, (queryE-queryS)/1000, (writeT-queryE)/1000, cnt);
							logger.info("###############################################");
						}
					}
					if(tb.equals("route") || tb.equals("status")){
						for(int j=0;j<24;j++){
							String hh="";
							if (j<10){
								hh="0"+j;
							}else{
								hh=""+j;
							}
							String table = "est_csr_"+tb+"_min";
							sql = "SELECT * FROM "+table+" WHERE dt='"+dt+"' and hh='"+hh+"'";
							logger.info("select hive table: {}",sql);
							long queryS = System.currentTimeMillis();
							hrs = hsm.executeQuery(sql);
							long queryE = System.currentTimeMillis();
							int cnt = write(hrs,table,dt,hh);
							long writeT = System.currentTimeMillis();
							logger.info("###############################################");
							logger.info("{} queryT: {}, writeT: {}, count: {}", table, (queryE-queryS)/1000, (writeT-queryE)/1000, cnt);
							logger.info("###############################################");
						}
					}
				}
				
				for(String tb : colTps){
					for(int j=0;j<24;j++){
						String hh="";
						if (j<10){
							hh="0"+j;
						}else{
							hh=""+j;
						}
						String table = "est_"+tb+"_min";
						sql = "SELECT * FROM "+table+" WHERE dt='"+dt+"' and hh='"+hh+"'";
						logger.info("select hive table: {}",sql);
						long queryS = System.currentTimeMillis();
						hrs = hsm.executeQuery(sql);
						long queryE = System.currentTimeMillis();
						int cnt = write(hrs,table,dt,hh);
						long writeT = System.currentTimeMillis();
						logger.info("###############################################");
						logger.info("{} queryT: {}, writeT: {}, count: {}", table, (queryE-queryS)/1000, (writeT-queryE)/1000, cnt);
						logger.info("###############################################");
					}
				}
				long dayE = System.currentTimeMillis();
				logger.info("###############################################");
				logger.info("{} Total: {}", dt, (dayE-dayS)/1000);
				logger.info("###############################################");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
	public static int write(ResultSet rs, String table, String dt, String hh){
		int cnt =0;
		try {
			ResultSetMetaData meta = rs.getMetaData();
			int fc = meta.getColumnCount();
			
			String column="";
			for(int j=0;j<fc;j++){
				String ff = meta.getColumnName(j+1);
				if(j+1==fc){
					column += ff+"\n";
				}else{
					column += ff+"|";
				}
			}
			StringBuilder sb = new StringBuilder();
			sb.append(column);
			while(rs.next()){
				for(int k=0;k<fc;k++){
					String ff = rs.getObject(k+1).toString();
					if(k+1==fc){
						sb.append(ff+"\n");
					}else{
						sb.append(ff+"|");
					}
				}
				cnt++;
			}
			if(sb.length()>0){
				fileWrite(sb,table,dt,hh);
				sb.setLength(0);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return cnt;
	}
	public static void fileWrite(StringBuilder sb, String table, String dt, String hh){
		try {
			String mk = rootPath+"/"+table+"/"+dt+"/";
			String path= mk+table+"_"+dt+"_"+hh+".csv";
			
			File f = new File(mk);
			if(!f.exists()){
				f.mkdirs();
			}
			
			BufferedWriter fw = new BufferedWriter(new FileWriter(path));
			fw.write(sb.toString());
			fw.flush();
			
			fw.close();
			logger.info("path: {}",path);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

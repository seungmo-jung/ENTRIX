package com.entrixco.cscenter.analysis.batch.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.entrixco.cscenter.analysis.batch.util.SqlMapper;

public class JdbcHour {
	
	private static final Logger logger = LoggerFactory.getLogger(JdbcHour.class);
	
	Connection hconn;
	Statement hsm;
	Connection mconn;
	PreparedStatement ps;
	ResultSet hrs;
	HashMap<String, String> sqlMap;
	String dt;
	String hh;
	String sd;
	HashMap<String, Object> configMap;
	
	public void connectJDBC(HashMap<String, Object> configMap){
		try {
			sqlMap = SqlMapper.loadSQLMap();
			dt = (String)configMap.get("dt");
			hh = (String)configMap.get("hh");
			sd = (String)configMap.get("sd");
			//hive connect
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			String hurl = configMap.get("default.hive.url").toString();
			String huser = configMap.get("default.hive.user").toString();
			String hpwd = configMap.get("default.hive.password").toString();
			hconn = DriverManager.getConnection(hurl,huser,hpwd);
			hsm = hconn.createStatement();
			
			//mysql connect
			Class.forName("com.mysql.jdbc.Driver");
			String murl = configMap.get("default.mysql.url").toString();
			String muser = configMap.get("default.mysql.user").toString();
			String mpwd = configMap.get("default.mysql.password").toString();
			mconn = DriverManager.getConnection(murl,muser,mpwd);
			mconn.setAutoCommit(false);
			
			this.configMap=configMap;
			
			logger.info(">>>>>>>connect JDBC");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public int insertMysql(String logtype, Boolean restore){
		int cnt = 0;
		String[] ar = null;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HH");
			SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
			SimpleDateFormat dtForm = new SimpleDateFormat("yyyyMMdd");
			SimpleDateFormat hhForm = new SimpleDateFormat("HH");
			
			Calendar cal = Calendar.getInstance();
			Date date = sdf.parse(sd);
			cal.setTime(date);
			cal.add(Calendar.HOUR, +1);
			Date enddate = cal.getTime();
			String startDate = dateForm.format(date);
			String endDate = dateForm.format(enddate);
			
			if(logtype.equals("Usetime")){
				try {
					if(restore){
						String[] use = {"est_css_usetime_hour", "est_app_usetime_hour", "est_so_usetime_hour"};
						for(String t:use){
							String del = "DELETE FROM "+t+" WHERE stat_hour='"+dt+hh+"'";
							ps = mconn.prepareStatement(del);
							logger.info(del);
							ps.executeUpdate();
						}
					}
					
					cal.add(Calendar.HOUR, -2);
					Date ago1 = cal.getTime();
					cal.add(Calendar.HOUR, -1);
					Date ago2 = cal.getTime();
					cal.add(Calendar.HOUR, -1);
					Date ago3 = cal.getTime();
					
					String ago1dt = dtForm.format(ago1);
					String ago1hh = hhForm.format(ago1);
					String ago2dt = dtForm.format(ago2);
					String ago2hh = hhForm.format(ago2);
					String ago3dt = dtForm.format(ago3);
					String ago3hh = hhForm.format(ago3);
					
					String sql = sqlMap.get("mysql@selectCssUsetimeH");
					sql = String.format(sql,dt,hh,ago1dt,ago1hh,ago2dt,ago2hh,ago3dt,ago3hh,startDate,endDate,dt+hh);
					hrs = hsm.executeQuery(sql);
					
					String isql = "insert into est_css_usetime_hour values ";
					while(hrs.next()){
						isql += "(";
						for(int i=1; i<42; i++){
							if(i<5){
								isql+="'"+hrs.getString(i)+"',";
							}else{
								if(i%2!=0){
									isql+=hrs.getLong(i)+",";
								}else{
									isql+=hrs.getFloat(i)+",";
								}
							}
						}
						isql += "now()),";
						cnt++;
					}
					isql=isql.substring(0, isql.length()-1);
					if(cnt>0){
						ps = mconn.prepareStatement(isql);
						ps.executeUpdate();
						logger.info(">>>>>>>>>>>>>>>>> insert css {} Stat cnt : {}",logtype,cnt);
						logger.info(sql);
						sql = sqlMap.get("mysql@selectAppUsetimeH");
						sql = String.format(sql,dt+hh);
						ps = mconn.prepareStatement(sql);
						int app = ps.executeUpdate();
						logger.info(">>>>>>>>>>>>>>>>> insert app {} Stat cnt : {}",logtype,app);
						logger.info(sql);
						sql = sqlMap.get("mysql@selectSoUsetimeH");
						sql = String.format(sql,dt+hh);
						ps = mconn.prepareStatement(sql);
						int so = ps.executeUpdate();
						logger.info(">>>>>>>>>>>>>>>>> insert so {} Stat cnt : {}",logtype,so);
						logger.info(sql);
						cnt = cnt+app+so;
					}else{
						logger.info(">>>>>>>>>>>>>>>>> insert css {} Stat cnt : {}",logtype,0);
						logger.info(sql);
						logger.info(">>>>>>>>>>>>>>>>> insert app {} Stat cnt : {}",logtype,0);
						logger.info(">>>>>>>>>>>>>>>>> insert so {} Stat cnt : {}",logtype,0);
					}
					
				} catch (Exception e) {
					e.printStackTrace();
				}
			}else if(logtype.equals("Uvpv")){
				try{
					if(restore){
						String[] use = {"est_css_uvpv_hour", "est_app_uvpv_hour", "est_so_uvpv_hour"};
						for(String t:use){
							String del = "DELETE FROM "+t+" WHERE stat_hour='"+dt+hh+"'";
							ps = mconn.prepareStatement(del);
							logger.info(del);
							ps.executeUpdate();
						}
					}
					for (int j=0;j<3;j++){
						String ty="";
						if(j==0) ty="Css";
						else if(j==1) ty="So";
						else if(j==2) ty="App";
						
						String sql = sqlMap.get("mysql@select"+ty+"Uvpv");
						String where = "dt='"+dt+"' and hh='"+hh+"'";
						sql = String.format(sql,dt+hh,where,startDate,endDate,where,startDate,endDate);
						hrs = hsm.executeQuery(sql);
						
						String isql = "insert into est_"+ty+"_uvpv_hour values ";
						int cntO=0;
						while(hrs.next()){
							isql += "(";
							for(int i=1; i<7-j; i++){
								if(i<5-j){
									isql+="'"+hrs.getString(i)+"',";
								}else{
									isql+=hrs.getLong(i)+",";
								}
							}
							isql += "now()),";
							cntO++;
						}
						isql=isql.substring(0, isql.length()-1);
						if(cntO>0){
							ps = mconn.prepareStatement(isql);
							ps.executeUpdate();
							logger.info(">>>>>>>>>>>>>>>>> insert {} {} Stat cnt : {}",ty,logtype,cntO);
							logger.info(sql);
							
							cnt += cntO;
						}else{
							logger.info(">>>>>>>>>>>>>>>>> insert {} {} Stat cnt : {}",ty,logtype,0);
							logger.info(sql);
						}
					}
					
					if(configMap.get("uvpv_day").equals("on") && hh.equals("00")){
						logger.info(">>>>>>>>>>>>>>Uvpv Day Start<<<<<<<<<<<<<<<<<");
						SimpleDateFormat dayF = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
						String endDay = dayF.format(date);
						cal.setTime(date);
						cal.add(Calendar.DATE, -1);
						Date startDayDate = cal.getTime();
						String startDay = dayF.format(startDayDate);
						String dtDay = dtForm.format(startDayDate);
						
						if(restore){
							String[] use = {"est_css_uvpv_day", "est_app_uvpv_day", "est_so_uvpv_day"};
							for(String t:use){
								String del = "DELETE FROM "+t+" WHERE stat_day='"+dtDay+"'";
								ps = mconn.prepareStatement(del);
								logger.info(del);
								ps.executeUpdate();
							}
						}
						
						for (int j=0;j<3;j++){
							String ty="";
							if(j==0) ty="Css";
							else if(j==1) ty="So";
							else if(j==2) ty="App";
							
							String sql = sqlMap.get("mysql@select"+ty+"Uvpv");
							String where = "dt='"+dtDay+"'";
							sql = String.format(sql,dtDay,where,startDay,endDay,where,startDay,endDay);
//							logger.info(sql);
							hrs = hsm.executeQuery(sql);
							
							String isql = "insert into est_"+ty+"_uvpv_day values ";
							int cntO=0;
							while(hrs.next()){
								isql += "(";
								for(int i=1; i<7-j; i++){
									if(i<5-j){
										isql+="'"+hrs.getString(i)+"',";
									}else{
										isql+=hrs.getLong(i)+",";
									}
								}
								isql += "now()),";
								cntO++;
							}
							isql=isql.substring(0, isql.length()-1);
							if(cntO>0){
								ps = mconn.prepareStatement(isql);
								ps.executeUpdate();
								logger.info(">>>>>>>>>>>>>>>>> insert DAY {} {} Stat cnt : {}",ty,logtype,cntO);
								logger.info(sql);
							}else{
								logger.info(">>>>>>>>>>>>>>>>> insert DAY {} {} Stat cnt : {}",ty,logtype,0);
								logger.info(sql);
							}
						}
					}
					
				}catch(Exception e){
					e.printStackTrace();
				}
			}else if(logtype.equals("CsrTps")||logtype.equals("WebTps")){
				if(restore){
					String[] use = null;
					if(logtype.equals("CsrTps")){
						use = new String[]{"est_csr_tps_hour", "est_so_csrtps_hour"};
					}else{
						use = new String[]{"est_web_tps_hour", "est_so_webtps_hour"};
					}
					for(String t:use){
						String del = "DELETE FROM "+t+" WHERE stat_hour='"+dt+hh+"'";
						ps = mconn.prepareStatement(del);
						logger.info(del);
						ps.executeUpdate();
					}
				}
				
				String sql = sqlMap.get("mysql@select"+logtype+"2");
				sql = String.format(sql,dt,hh);
				hrs = hsm.executeQuery(sql);
				
				String table = configMap.get("jdbc."+logtype+".table").toString();
				String[] type = configMap.get("jdbc."+logtype+".type").toString().split(",");
				
				String isql = "insert into "+table+" values ";
				while(hrs.next()){
					isql += "(";
					for(int i=0; i<type.length; i++){
						if(type[i].equals("String")){
							isql+="'"+hrs.getString(i+1)+"',";
						}else if(type[i].equals("Float")){
							isql+=hrs.getFloat(i+1)+",";
						}else if(type[i].equals("Long")){
							isql+=hrs.getLong(i+1)+",";
						}else{
							isql+=hrs.getObject(i+1)+",";
						}
					}
					isql += "now()),";
					cnt++;
				}
				isql=isql.substring(0, isql.length()-1);
				int so=0;
				if(cnt>0){
					ps = mconn.prepareStatement(isql);
					ps.executeUpdate();
					logger.info(">>>>>>>>>>>>>>>>> insert host {} Stat cnt : {}",logtype,cnt);
					logger.info(sql);
					sql = sqlMap.get("mysql@selectSo"+logtype);
					sql = String.format(sql,dt+hh);
					ps = mconn.prepareStatement(sql);
					so = ps.executeUpdate();
					cnt = cnt+so;
					logger.info(">>>>>>>>>>>>>>>>> insert so {} Stat cnt : {}",logtype,so);
				}else{
					logger.info(">>>>>>>>>>>>>>>>> insert host {} Stat cnt : {}",logtype,0);
					logger.info(sql);
					logger.info(">>>>>>>>>>>>>>>>> insert so {} Stat cnt : {}",logtype,0);
				}
				
			}else{
				if(logtype.equals("Route")||logtype.equals("Status")){
					ar = new String[] {"Csr","App","So","Css"};
				}else if(logtype.equals("Cpu")||logtype.equals("Mem")||logtype.equals("Disk")||logtype.equals("Network")||logtype.equals("Uplink")){
					ar = new String[] {""};
				}else{
					ar = new String[] {"App","So","Css"};
				}
				for(String t : ar){
					if(restore){
						String tt=t;
						if(!t.equals("")){
							tt=t+"_";
						}
						String del = "DELETE FROM est_"+tt+logtype+"_hour WHERE stat_hour='"+dt+hh+"'";
						ps = mconn.prepareStatement(del);
						logger.info(del);
						ps.executeUpdate();
					}
					
					String table = (String)configMap.get("jdbc."+t+logtype+".table");
					String[] fieldtype = configMap.get("jdbc."+logtype+".type").toString().split(",");
					
					String sql = sqlMap.get("mysql@select"+t+logtype);
					sql = String.format(sql,dt,hh);
					hrs = hsm.executeQuery(sql);
					
					String isql = "insert into "+table+" values ";
					while(hrs.next()){
						isql += "(";
						int in = 1;
						if(t.equals("Csr")||t.equals("App")){
							isql += "'"+hrs.getString(1)+"','"+hrs.getString(2)+"',";
							in = 3;
						}else if(t.equals("So")){
							isql += "'"+hrs.getString(1)+"','"+hrs.getString(2)+"','"+hrs.getString(3)+"',";
							in = 4;
						}else if(t.equals("Css")){
							isql += "'"+hrs.getString(1)+"','"+hrs.getString(2)+"','"+hrs.getString(3)+"','"+hrs.getString(4)+"',";
							in = 5;
						}
						for(int i=0;i<fieldtype.length;i++){
							if(fieldtype[i].equals("String")){
								isql += "'"+hrs.getString(i+in)+"',";
							}else if(fieldtype[i].equals("Float")){
								isql += hrs.getFloat(i+in)+",";
							}else if(fieldtype[i].equals("Int")){
								isql += hrs.getInt(i+in)+",";
							}else if(fieldtype[i].equals("Long")){
								isql += hrs.getLong(i+in)+",";
							}else{
								isql += hrs.getObject(i+in)+",";
							}
						}
						isql += "now()),";
						cnt++;
					}
					isql=isql.substring(0, isql.length()-1);
					if(cnt>0){
						ps = mconn.prepareStatement(isql);
						ps.executeUpdate();
					}
					logger.info(">>>>>>>>>>>>>>>>> insert {} Stat cnt : {}",t+logtype,cnt);
					logger.info(sql);
				}
			}
				
		} catch (Exception e) {
			e.printStackTrace();
		}
		return cnt;
	}
	public void closeJDBC(){
		try {
			if(ps!=null){
				ps.executeBatch();
				ps.close();
			}
			if(mconn!=null){
				mconn.commit();
				mconn.close();
			}
			if(hrs!=null)hrs.close();
			if(hsm!=null)hsm.close();
			if(hconn!=null)hconn.close();
			logger.info(">>>>>>>close JDBC");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void deleteSpark(String dt){
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(sdf.parse(dt));
			
			cal.add(Calendar.DATE, -7);
			String dt7 = sdf.format(new Date(cal.getTimeInMillis()));
			cal.add(Calendar.DATE, -173);
			String dt180 = sdf.format(new Date(cal.getTimeInMillis()));
			
			String[] type = {"css", "so", "app"}; 
			String[] css = {"bandwidth", "ccu", "uvpv", "route", "status", "sed", "web"};
			String[] colTps = {"cpu", "disk", "mem", "network", "uplink", "csr_tps", "so_csrtps", "web_tps", "so_webtps"};
			String[] log = {"tb_csr_r", "tb_csr_s", "tb_onoff", "tb_sed", "tb_cpu", "tb_disk", "tb_mem", "tb_network"};
			String[] web = {"tb_csr_web", "tb_web"};
			
			String sql;
			for(String tb : css){
				for(String t : type){
					sql = "ALTER TABLE est_"+t+"_"+tb+"_min DROP IF EXISTS PARTITION (dt<='"+dt180+"')";
					logger.info("delete hive table: {}",sql);
					hsm.executeQuery(sql);
				}
				if(tb.equals("route") || tb.equals("status")){
					sql = "ALTER TABLE est_csr_"+tb+"_min DROP IF EXISTS PARTITION (dt<='"+dt180+"')";
					logger.info("delete hive table: {}",sql);
					hsm.executeQuery(sql);
				}
			}
			
			for(String tb : colTps){
				sql = "ALTER TABLE est_"+tb+"_min DROP IF EXISTS PARTITION (dt<='"+dt180+"')";
				logger.info("delete hive table: {}",sql);
				hsm.executeQuery(sql);
			}
			for(String tb : log){
				sql = "ALTER TABLE "+tb+" DROP IF EXISTS PARTITION (dt<='"+dt180+"')";
				logger.info("delete hive table: {}",sql);
				hsm.executeQuery(sql);
			}
			for(String tb : web){
				sql = "ALTER TABLE "+tb+" DROP IF EXISTS PARTITION (dt<='"+dt7+"')";
				logger.info("delete hive table: {}",sql);
				hsm.executeQuery(sql);
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}



// csrtps webtps 분통계 없이 바로 시통계 계산
//String sql = sqlMap.get("mysql@select"+logtype);
//sql = String.format(sql,dt+hh,dt,hh,startDate,endDate);
//hrs = hsm.executeQuery(sql);
//
//String table = configMap.get("jdbc."+logtype+".table").toString();
//String[] type = configMap.get("jdbc."+logtype+".type").toString().split(",");
//
//String isql = "insert into "+table+" values ";
//while(hrs.next()){
//	isql += "(";
//	for(int i=0; i<type.length; i++){
//		if(type[i].equals("String")){
//			isql+="'"+hrs.getString(i+1)+"',";
//		}else if(type[i].equals("Float")){
//			isql+=hrs.getFloat(i+1)+",";
//		}else if(type[i].equals("Long")){
//			isql+=hrs.getLong(i+1)+",";
//		}else{
//			isql+=hrs.getObject(i+1)+",";
//		}
//	}
//	isql += "now()),";
//	cnt++;
//}
//isql=isql.substring(0, isql.length()-1);
//if(cnt>0){
//	ps = mconn.prepareStatement(isql);
//	ps.executeUpdate();
//	logger.info(">>>>>>>>>>>>>>>>> insert host {} Stat cnt : {}",logtype,cnt);
//	sql = sqlMap.get("mysql@selectSo"+logtype);
//	sql = String.format(sql,dt+hh);
//	ps = mconn.prepareStatement(sql);
//	int so = ps.executeUpdate();
//	logger.info(">>>>>>>>>>>>>>>>> insert so {} Stat cnt : {}",logtype,so);
//	cnt = cnt+so;
//}

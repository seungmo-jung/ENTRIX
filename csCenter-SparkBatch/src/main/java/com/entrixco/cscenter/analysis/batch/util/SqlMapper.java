package com.entrixco.cscenter.analysis.batch.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class SqlMapper {
	
	private static final Logger logger = LoggerFactory.getLogger(SqlMapper.class);
	
	public static HashMap<String, String> loadSQLMap() {
		URL sqlUrl = Thread.currentThread()
				.getContextClassLoader().getResource(BatchConfig.SQL_DIR);
		logger.info("sqlUrl={}", sqlUrl);
		if(sqlUrl==null|| !new File(sqlUrl.getFile()).exists()) {
			return loadSQLMapFromJar();
		}
		HashMap<String, String> xmap = new HashMap<String, String>();
		File sqlDir = new File(sqlUrl.getFile());
		logger.info("sqlDir : exists={}, path={}", sqlDir.exists(), sqlDir.getAbsolutePath());
		File[] files = sqlDir.listFiles();
		for(File file : files) {
			if(file.isFile() && file.getName().toLowerCase().endsWith(".xml")) {
				readXmlToSQLMap(xmap, file.getAbsolutePath());
			}
		}
		return xmap;
	}
	
	public static HashMap<String, String> loadSQLMapFromJar() {
		HashMap<String, String> xmap = new HashMap<String, String>();
		CodeSource codesrc = SqlMapper.class.getProtectionDomain().getCodeSource();
		URL jarUrl = codesrc.getLocation();
		logger.info("jarUrl={}", jarUrl);
		ArrayList<String> xlist = new ArrayList<String>();
		ZipInputStream zis = null;
		try {
			zis = new ZipInputStream(jarUrl.openStream());
			ZipEntry ze = null;
			while( (ze=zis.getNextEntry())!=null ) {
				String entry = ze.getName();
				if(entry.startsWith(BatchConfig.SQL_DIR)
						&& entry.toLowerCase().endsWith(".xml")) {
					xlist.add(entry);
				}
			}
		} catch(Exception e) {
			logger.error("loadSQLMapFromJar", e);
			throw new RuntimeException(e);
		} finally {
			if(zis!=null) {
				try {zis.close();} catch(Exception fe) {}
			}
		}
		for(String xurl : xlist) {
			readXmlToSQLMap(xmap, xurl);
		}
		return xmap;
	}
	
	private static HashMap<String, String> readXmlToSQLMap(
			HashMap<String, String> xmap, String path) {
		if(xmap==null) xmap = new HashMap<String, String>();
		
		InputStream xis = null;
		try {
			File xfile = new File(path);
			if(xfile.exists()) {
				xis = new FileInputStream(xfile);
			}
			else {
				xis = Thread.currentThread()
						.getContextClassLoader().getResourceAsStream(path);
			}
			DocumentBuilder builder =
					DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document xml = builder.parse(xis);
			String namespace = xml.getDocumentElement().getNodeName();
			boolean isDefaultNamespace = namespace.equals("default");
			
			NodeList nlist = xml.getDocumentElement().getChildNodes();
			String qname = null;
			for(int n=0, length=nlist.getLength(); n<length; n++) {
				Node node = nlist.item(n);
				if(node.getNodeType()==Node.ELEMENT_NODE) {
					qname = isDefaultNamespace ?
							node.getNodeName() : namespace+"@"+node.getNodeName();
					xmap.put(qname, node.getTextContent());
				}
			}
			logger.info("read xml sql : size={}, namespace={}, path={}"
					, xmap.size(), namespace, path);
			return xmap;
		} catch(Exception e) {
			logger.error("readXmlToSQLMap", e);
			throw new RuntimeException(e);
		} finally {
			if(xis!=null) {
				try {xis.close();} catch(Exception e) {}
			}
		}
	}

}

package com.entrixco.cscenter.analysis.batch;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;

public class ESTest {

	public static void main(String[] args) {
		String a = "201610111413";
		
		System.out.println(a.substring(0,8));
		System.out.println(a.substring(8,10));
//		try {
//			SimpleDateFormat jdate = new SimpleDateFormat("yyyy:hh:mm:ss");
//			SimpleDateFormat hdate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",Locale.KOREA);
//			String csr_date = "2016:00:09:23";
//			Date jDate = jdate.parse(csr_date);
//			csr_date = hdate.format(jDate);
//			System.out.println(csr_date);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		
		
//		try {
//			
//			Settings settings = Settings.settingsBuilder().put("cluster.name", "entrixes").build();
//			TransportClient client = TransportClient.builder().settings(settings).addPlugin(DeleteByQueryPlugin.class).build()
//					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("ent-dev-agg01"), 9300))
//					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("ent-dev-agg02"), 9300))
//					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("ent-dev-agg03"), 9300));
//			
//			StringBuilder b = new StringBuilder("");
//		    b.append("{");
//		    b.append("  \"query\": {");  
//		    b.append("      \"range\": {");
//		    b.append("          \"stat_min\": { \"gte\":\"201609301135\", \"lt\":\"201609301140\" }"  );
//		    b.append("      }");
//		    b.append("  }");
//		    b.append("}");
//			
//			DeleteByQueryResponse response = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
//					.setIndices("est_cpu_min").setTypes("docs").setSource(b.toString()).execute().actionGet();
//			System.out.println(response.toString());
//			
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
	}

}
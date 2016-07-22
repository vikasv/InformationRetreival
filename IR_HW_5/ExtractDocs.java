
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;

public class ExtractDocs {
	public static void main(String[] args) throws IOException, InterruptedException
	{
		Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
				.put("cluster.name","mvk_cluster").build();
	
        Client client = TransportClient.builder().settings(settings).build()
        		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
        BulkRequestBuilder breq=client.prepareBulk(); 
        

        List<Object> url = new ArrayList<Object>();
        
        for(int i=1; i<151;i++){
        	
        SearchResponse response = client.prepareSearch("legend")
				.setTypes("doc") 
				.setQuery(QueryBuilders.termQuery("_id",i))
				.addScriptField("get_url", (new Script("doc['Url'].value")))
				.addScriptField("get_author", (new Script("doc['Author'].value")))
				.addScriptField("get_queryid", (new Script("doc['Query_Word'].value")))
				.addScriptField("get_score", (new Script("doc['Score'].value")))
				 .setScroll(new TimeValue(60000))
				 .setSize(100).execute().actionGet();
        
		 long hits = response.getHits().getTotalHits();
		//System.out.println("hit count "+hits);
		
		

		    for (SearchHit hit : response.getHits().getHits()) {
		    	
		        //System.out.print(hit.getId()+" ");
		        System.out.print(151704+" ");
		        
		        //System.out.print( hit.getFields().get("get_title").getValue()+" ");
		        System.out.print((String) hit.getFields().get("get_author").getValue()+" ");
		        System.out.print((String) hit.getFields().get("get_url").getValue()+" ");
		        //System.out.print(hit.getFields().get("get_queryid").getValue()+" ");
		        System.out.println( hit.getFields().get("get_score").getValue()+" ");
		    }
		   
		
	}
	}
}

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;

public class NewIndexRanker {
	
	public static Map<String, Double> authoritiesmap= new LinkedHashMap<String,Double>();
	public static Map<String, Double> hubsmap = new LinkedHashMap<String,Double>();
	public static HashMap<String,Integer> incount = new HashMap<String,Integer>();
	public static HashMap<String,Integer> outcount = new HashMap<String,Integer>();
	
	public static void get1000Docs() throws IOException
	{
		Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
				.put("cluster.name","mvk_cluster").build();
		Client client = TransportClient.builder().settings(settings).build()
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
		SearchResponse response = client.prepareSearch("final_site")
				.setTypes("document")
                .setScroll(new TimeValue(6000))
                .setQuery("{\"match\": {\"text\": \""+ "data structures" +"\"}}")
                .addFields("docno","outlinks","inlinks")
                .setSize(1000)
                .setExplain(true)
                .execute()
                .actionGet();
		BufferedWriter outLinksFile = new BufferedWriter(new FileWriter(new File("outLinks.txt")));
		BufferedWriter inLinksFile = new BufferedWriter(new FileWriter(new File("inLinks.txt")));
		BufferedWriter rootFile = new BufferedWriter(new FileWriter(new File("rootFile.txt")));
		int count = 0;
		String docNo;
        String inLinks;
        String outLinks;
        StringBuilder inLinksString;
        for(SearchHit hit : response.getHits().getHits())
        {
        	count++;
        	docNo = "";
        	inLinks = "";
        	outLinks = "";
        	docNo = hit.field("docno").<String>getValue();
        	rootFile.write(docNo);
        	rootFile.newLine();
        	inLinks = hit.getFields().get("getinlinks").getValue();
        	String inlinks[] = inLinks.split(",");
        	inLinksString = new StringBuilder();
			for(int i=0;i<inlinks.length;i++)
			{
				inlinks[i] = UtilCanonical.cannonical_url(inlinks[i]);
				inLinksString.append(inlinks[i]+" ");
			}
			inLinksFile.write(docNo+" "+inLinksString.toString());
			inLinksFile.newLine();
			outLinks = hit.getFields().get("getoutlinks").getValue();
			outLinksFile.write(docNo+" "+outLinks);
			outLinksFile.newLine();
			
			if(count == 1000)
			{
				return;
			}
        	
        }
		outLinksFile.close();
		inLinksFile.close();
	
	}
	
	public static void baseFile() throws IOException
	{
		BufferedWriter bFile = new BufferedWriter(new FileWriter(new File("baseFile.txt")));
		LinkedHashSet<String> baseSet = new LinkedHashSet<String>();
		BufferedReader  rFile= new BufferedReader(new FileReader("rootFile.txt"));
		BufferedReader  inLinksFile= new BufferedReader(new FileReader("inLinks.txt"));
		BufferedReader  outLinksFile= new BufferedReader(new FileReader("outLinks.txt"));
		String rline = "";
		String inline = "";
		String outline = "";
		while((rline = rFile.readLine()) != null && baseSet.size() < 10000) {
			baseSet.add(rline);
			while((inline = inLinksFile.readLine()) != null){
				String url = inline.split(" ")[0];
				if (url.equals(rline))
				{
					String[] inLinkString = inline.split(" ")[1].split(", ");
					for(String link: inLinkString)
					{
						link = link.toLowerCase().trim();
						int randomCount = 0;
						if(link.matches("^[\u0000-\u0080]+$"))
						{
							randomCount++;
							baseSet.add(UtilCanonical.cannonical_url(link));
						}
						if( randomCount == 6)
						{
							break;
						}
					}
				}
			}
			while((outline = outLinksFile.readLine()) != null){
				String url = outline.split(" ")[0];
				if (url.equals(rline))
				{
					String[] outLinkString = outline.split(" ")[1].split(",");
					for(String link: outLinkString)
					{
						link = link.toLowerCase().trim();
						int randomCount = 0;
						if(link.matches("^[\u0000-\u0080]+$"))
						{
							randomCount++;
							baseSet.add(link);
						}
						if( randomCount == 5)
						{
							break;
						}
					}
				}
			}
		}
		
		for(String link: baseSet)
		{
			bFile.write(link);
			bFile.newLine();
		}
		
		bFile.close();
		rFile.close();
		inLinksFile.close();
		outLinksFile.close();
	}
	
	public static void IndexFile(String fName) throws IOException, InterruptedException
	{
		Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
				.put("cluster.name","mvk_cluster").build();
		Client client = TransportClient.builder().settings(settings).build()
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));	
		SearchResponse searchResponse =  client.prepareSearch("final_site")
				.setTypes("document")
				.setSearchType(SearchType.SCAN) 
				.setQuery(QueryBuilders.matchAllQuery())
				.addScriptField("getinlinks", (new Script("doc['in_links'].value")))
				.setScroll(new TimeValue(60000))
				.setSize(100).execute().actionGet();
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(fName)));
		StringBuilder sb;
		while (searchResponse.getHits().getHits().length != 0) {
			for (SearchHit hit : searchResponse.getHits().getHits())
			{
				String inlinkStr = hit.getFields().get("getinlinks").getValue();
				String inlinks[] = inlinkStr.split(",");
				sb = new StringBuilder();
				for(int i=0;i<inlinks.length;i++)
				{
					inlinks[i] = UtilCanonical.cannonical_url(inlinks[i]);
					sb.append(inlinks[i]+" ");
				}
				inlinkStr = sb.toString();				
				bw.write(hit.getId()+" "+inlinkStr);
				bw.newLine();
			}
			searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
		}
		bw.close();
	}
	
	public static void hubsAuthorities() throws IOException
	{

		BufferedReader indexFileRead= new BufferedReader(new FileReader("indexFile.txt"));
		String line = "";
		while((line = indexFileRead.readLine()) != null)
		{
			String url = line.split(" ")[0].trim();
			//graph.put(url, new Tuple<Double,Double>(1.0,1.0));
			authoritiesmap.put(url, 1.0);
			hubsmap.put(url, 1.0);
			
		}
		BufferedReader  rFile= new BufferedReader(new FileReader("rootFile.txt"));
		HashSet<String> rootSet = new HashSet<String>();
		String rline = "";
		while((rline = rFile.readLine())!=null)
		{
			rootSet.add(rline.trim());
		}
		BufferedReader  inLinksFile= new BufferedReader(new FileReader("inLinks.txt"));
		String inline = "";
		HashMap<String,String> inLinksHM = new HashMap<String,String>();
		while((inline = inLinksFile.readLine())!=null)
		{
			String url = inline.split(" ")[0].trim();
			String inlinksStr = inline.split(" ")[1].trim();
			inLinksHM.put(url,inlinksStr);
		}
		BufferedReader  outLinksFile= new BufferedReader(new FileReader("outLinks.txt"));
		String outline = "";
		HashMap<String,String> outLinksHM = new HashMap<String,String>();
		while((outline = outLinksFile.readLine())!=null)
		{
			String url = outline.split(" ")[0].trim();
			String outlinksStr = outline.split(" ")[1].trim();
			outLinksHM.put(url,outlinksStr);
		}		
		BufferedReader bFile = new BufferedReader(new FileReader("baseFile.txt"));
		HashSet<String> baseSet = new HashSet<String>();
		String bline = "";
		while((bline = bFile.readLine())!=null)
		{
			baseSet.add(bline.trim());
		}
		double authorityNormal;
		double hubNormal;
		double check = (double)Math.pow(10, -5);
		List<Double> authorities = new ArrayList<Double>();
		int length = authorities.size();
		double authorities1 = Math.abs(authorities.get(length-2)-authorities.get(length-1));
		double authorities2 = Math.abs(authorities.get(length-3)-authorities.get(length-2));
		double authorities3 = Math.abs(authorities.get(length-4)-authorities.get(length-3));
		boolean authorityConverged = ((length >3) && (authorities1 < check) && (authorities2 < check) && (authorities3 < check));
		List<Double> hubs = new ArrayList<Double>();
		int size = hubs.size();
		double hubs1 = Math.abs(hubs.get(size-2)-hubs.get(size-1));
		double hubs2 = Math.abs(hubs.get(size-3)-hubs.get(size-2));
		double hubs3 = Math.abs(hubs.get(size-4)-hubs.get(size-3));		
		boolean hubConverged = ((size >3) && (hubs1 < check) && (hubs2 < check) && (hubs3 < check));
		while(!(authorityConverged && hubConverged))
		{
			authorityNormal = 0.0;
			for(String url: rootSet)
			{
				double authScore = 0.0;
				if (inLinksHM.containsKey(url))
				{
					String[] inlinksArr = inLinksHM.get(url).split(",");
					incount.put(url, inlinksArr.length);
					for(String inlink : inlinksArr)
					{
						if(authoritiesmap.containsKey(inlink))
						{
							authScore = authScore + authoritiesmap.get(inlink);
						}
					}
					authorityNormal = authorityNormal + (authScore * authScore);
					if(authoritiesmap.containsKey(UtilCanonical.cannonical_url(url)))
					{
						authoritiesmap.put(UtilCanonical.cannonical_url(url), authScore);
					}
				}	
			}
			authorityNormal = Math.sqrt(authorityNormal);
			for(String url:rootSet)
			{
				authoritiesmap.put(url,(authoritiesmap.get(url)/authorityNormal));
			}
			authorities.add(authorityNormal);
			hubNormal = 0.0;
			for(String url: baseSet)
			{
				double hubScore = 0.0;
				if (inLinksHM.containsKey(url))
				{
					String[] outlinksArr = outLinksHM.get(url).split(",");
					outcount.put(url, outlinksArr.length);
					for(String outlink : outlinksArr)
					{
						if(hubsmap.containsKey(outlink))
						{
							hubScore = hubScore + hubsmap.get(outlink);
						}
					}
					hubNormal = hubNormal + (hubScore * hubScore);
					if(hubsmap.containsKey(UtilCanonical.cannonical_url(url)))
					{
						hubsmap.put(UtilCanonical.cannonical_url(url), hubScore);
					}
				}	
			}
			hubNormal = Math.sqrt(hubNormal);
			for(String url:baseSet)
			{
				hubsmap.put(url, (hubsmap.get(url)/hubNormal));
			} 
			hubs.add(hubNormal);
			
		}
	}
	
	public static void hubsandAuthoritiesWrite() throws IOException
	{
		BufferedReader  rFile= new BufferedReader(new FileReader("rootFile.txt"));
		HashSet<String> rootSet = new HashSet<String>();
		String rline = "";
		while((rline = rFile.readLine())!=null)
		{
			rootSet.add(rline.trim());
		}
		BufferedReader bFile = new BufferedReader(new FileReader("baseFile.txt"));
		HashSet<String> baseSet = new HashSet<String>();
		String bline = "";
		while((bline = bFile.readLine())!=null)
		{
			baseSet.add(bline.trim());
		}

		PrintWriter authorityPW = new PrintWriter(new File("Authority.txt"));
		PrintWriter hubPW = new PrintWriter(new File("Hub.txt"));
		
		authoritiesmap = sortMap(authoritiesmap);
		hubsmap=sortMap(hubsmap);
		
		int authCount = 1;
		for (Entry<String, Double> entry : authoritiesmap.entrySet()) {
			if(authCount <= 500)
			{
				authorityPW.println(entry.getKey()+"	"+entry.getValue()+"	"+incount.get(entry.getKey()));
			}
			authCount++;
		}
		int hubCount = 1;
		for (Entry<String, Double> entry : hubsmap.entrySet()) {
			if(hubCount <= 500)
			{
				hubPW.println(entry.getKey()+"	"+entry.getValue()+"	"+outcount.get(entry.getKey()));
			}
			hubCount++;
		}
	}
		
	public static void main(String[] args) throws IOException, InterruptedException {
		newPageRank p = new newPageRank();
		// TODO Auto-generated method stub
		//IndexFile("indexFile.txt");
		//scores("indexFile.txt");
		p.calculatePageRankPerplexity(newPageRank.createGraphFromFile("wt2g_inlinks.txt"));
		//get1000Docs();
		//baseFile();
		//hubsandAuthoritiesWrite();

	}
	
	public static Map<String, Double> sortMap(Map<String,Double> map)
	{
		List<Entry<String,Double>> list = new LinkedList<>(map.entrySet());
		Collections.sort(list, new Comparator<Object>() {
			@SuppressWarnings("unchecked")
			public int compare(Object o1, Object o2) {
				return ((Comparable<Double>) ((Map.Entry<String,Double>) (o1)).getValue()).compareTo(((Map.Entry<String,Double>) (o2)).getValue());
			}
		});
		Map<String,Double> sortedMap = new LinkedHashMap<>();
	    for (Iterator<Entry<String,Double>> it = list.iterator(); it.hasNext();) {
	        Map.Entry<String,Double> entry = (Map.Entry<String,Double>) it.next();
	        sortedMap.put(entry.getKey(), entry.getValue());
	    }
		return sortedMap;
	}

}

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.lucene.queries.payloads.AveragePayloadFunction;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class Query {
	static HashMap<String, HashSet<String>> queryMap = new HashMap<String, HashSet<String>>();
	public static String cleaningQuery(String line)
	{
		String queryNo = "";
		// still need to remove stop words, extra words in each query
		// stemming words
		if (line.length() > 0)
		{
			String[] queryDivision = line.split(".   ");
			HashSet<String> hs = new HashSet<String>();
			String query = queryDivision[1].toLowerCase(); 
			query = query.replaceAll("\\(", "");
			query = query.replaceAll("\\)", "");
			query = query.replaceAll("\\,", "");
			query = query.replaceAll("\\-", "");
			query = query.replaceAll("\"", "");
			String[] words = query.split(" ");
			
			//add words to a hashSet to remove duplicates
			for(String word: words)
			{
				hs.add(word.trim());
			}
			queryMap.put(queryDivision[0],hs);
			queryNo = queryDivision[0];
			
			
		}
		return queryNo;	
	}
	
	public static SearchHits getResponse(String word)
	{
		// all the words
		String queryId;
		double termFreq;
		String docno;
		double docLength; 
		double avgLengthDoc = 10000;
		double okapiScore;
		Client client;
		int size = 10000;
		SearchHits hits = null;
		double sumOfOkapiHitsPerWord = 0.0 ;
		try {
			client = TransportClient.builder().build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

			final Map<String, Object> params = new HashMap<>();
			params.put("term", word);
			params.put("field", "text");

			SearchResponse response = client.prepareSearch("ap_dataset")
					.setTypes("document")
					.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
					.setQuery(QueryBuilders.functionScoreQuery
							(QueryBuilders.termQuery("text", word), 
									new ScriptScoreFunctionBuilder(new Script("getTF", 
											ScriptType.INDEXED, "groovy", params)))
							.boostMode("replace"))
					.setFrom(0)
					.setSize(size)
					.execute()
					.actionGet();
			hits = response.getHits();
			okapiTFNew(hits);
			client.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		//return sumOfOkapiHitsPerWord;
		return hits;
	}
	
	public static double okapiTFNew(SearchHits hits)
	{		
		String queryId;
		double termFreq;
		String docno;
		double docLength;
		double sumOfOkapiHitsPerWord = 0.0 ;
		double avgLengthDoc = 10000;
		for (SearchHit hit: hits)
		{
			//System.out.println(hit.getId() + "-" + hit.getScore() + " - " + hit.getSource().get("docno")+" - "+hit.getSource().get("text").toString().length());
			queryId = hit.getId();
			termFreq = hit.getScore();
			docno = (String) hit.getSource().get("docno");
			docLength = hit.getSource().get("text").toString().length();
			double okapiTFperHit = termFreq/(termFreq + 0.5 + (1.5 * ((docLength)/(avgLengthDoc))));
			sumOfOkapiHitsPerWord += okapiTFperHit;
		}
		return sumOfOkapiHitsPerWord;
	}
	
	public static void readQueries(File filename) throws IOException
	{
		String line = null;
		FileReader fileReader = new FileReader(filename);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		
		
		while((line = bufferedReader.readLine()) != null) {
			String queryNo = cleaningQuery(line);
			HashSet wordsInQuery = queryMap.get(queryNo);
			double okapiTFperQuery = 0.0;
			for(Object wordInQuery : wordsInQuery)
			{
				SearchHits hits = getResponse((String)wordInQuery);
				okapiTFperQuery += okapiTFNew(hits);
			}
			
			System.out.println("OkapiTF for queryNO :"+queryNo +" : "+okapiTFperQuery);
			
		}   
		bufferedReader.close();
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		File queryFile = new File("C://InformationRetrieval//AP89_DATA//AP_DATA//query_desc.51-100.short - Copy.txt");
		readQueries(queryFile);
		//Query.getResponse("iran");
	}
}

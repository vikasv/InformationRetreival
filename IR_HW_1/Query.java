import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.tartarus.snowball.*;
import org.tartarus.snowball.ext.EnglishStemmer;
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
	static EnglishStemmer engstem = new EnglishStemmer();
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
			
			for(String word: words)
			{
				engstem.setCurrent(word);
				if (engstem.stem())
				{
				hs.add(engstem.getCurrent());
				}
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
		double avgLengthDoc = 247;
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
			//System.out.println(response);
			hits = response.getHits();
			//System.out.println(hits.getTotalHits());
			//okapiTFNew(hits);
			client.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		//return sumOfOkapiHitsPerWord;
		return hits;
	}
	
	public static void okapiTFNew(SearchHits hits,HashMap<String,Double> okapiScores)
	{		
		String queryId;
		double termFreq;
		String docno;
		double docLength;
		double sumOfOkapiHitsPerWord = 0.0 ;
		double avgLengthDoc = 247.88;
		
		for (SearchHit hit: hits)
		{
			//System.out.println(hit.getId() + "-" + hit.getScore() + " - " + hit.getSource().get("docno")+" - "+hit.getSource().get("text").toString().length());
			queryId = hit.getId();
			termFreq = hit.getScore();
			docno = (String) hit.getSource().get("docno");
			docLength = hit.getSource().get("text").toString().split(" ").length;
			//System.out.println(docLength);
			double okapiTFperHit = termFreq/(termFreq + 0.5 + (1.5 * ((docLength)/(avgLengthDoc))));
			if(!okapiScores.containsKey(docno))
				okapiScores.put(docno,okapiTFperHit);
			else
				okapiScores.put(docno,okapiScores.get(docno)+okapiTFperHit);
		}
		//System.out.println("okapihitsperword" + sumOfOkapiHitsPerWord);
	}
	
	public static void TFIDFNew(SearchHits hits,HashMap<String,Double> scores)
	{
		String queryId;
		double termFreq = 0.0;
		String docno;
		double docLength = 0.0;
		long totalDocLen = 84679;
		double sumOfTFIDFHitsPerWord = 0.0 ;
		double avgLengthDoc = 247.88;
		double dfw=hits.getTotalHits();
		System.out.println(dfw);
		double TFIDF;

		for (SearchHit hit: hits)
		{
			//System.out.println(hit.getId() + "-" + hit.getScore() + " - " + hit.getSource().get("docno")+" - "+hit.getSource().get("text").toString().length());
			queryId = hit.getId();
			termFreq = hit.getScore();
			docno = (String) hit.getSource().get("docno");
			docLength = hit.getSource().get("text").toString().split(" ").length;
			double TFIDFperHit = termFreq/(termFreq + 0.5 + (1.5 * ((docLength)/(avgLengthDoc)))) * Math.log(totalDocLen/dfw);
			if(!scores.containsKey(docno))
				scores.put(docno,TFIDFperHit);
			else
				scores.put(docno,scores.get(docno)+TFIDFperHit);
		}
	}
	
	public static void OkapiBM25(SearchHits hits,HashMap<String,Double> scores)
	{
		String queryId;
		double termFreq = 0.0;
		String docno;
		int docLength = 0;
		long totalDocLen = 84679;
		double sumOfOkapiBM25HitsPerWord = 0.0 ;
		double avgLengthDoc = 247.88;
		double dfw=hits.getTotalHits();
		double OkapiBM25;
		int TFWD = 1;
		double k1 = 1.2;
		int k2 = 100;
		double b = 0.75;
		for (SearchHit hit: hits)
		{
			queryId = hit.getId();
			termFreq = hit.getScore();
			docno = (String) hit.getSource().get("docno");
			docLength = hit.getSource().get("text").toString().split(" ").length;
			//System.out.println(docLength);
			double okapiBM25perHit = (termFreq+(k1*termFreq))/(termFreq+ (k1*((1-b)+(b*docLength/avgLengthDoc)))) * Math.log((totalDocLen+0.5)/(dfw+0.5));
			
			if(!scores.containsKey(docno))
				scores.put(docno,okapiBM25perHit);
			else
				scores.put(docno,scores.get(docno)+okapiBM25perHit);
		}
		
	}
	
	public static void laplaceScore(SearchHits hits,HashMap<String,Double> scores)
	{		
		String queryId;
		double termFreq;
		String docno;
		double docLength;
		double sumOfOkapiHitsPerWord = 0.0 ;
		double V = 173377.0;
		
		for (SearchHit hit: hits)
		{
			//System.out.println(hit.getId() + "-" + hit.getScore() + " - " + hit.getSource().get("docno")+" - "+hit.getSource().get("text").toString().length());
			queryId = hit.getId();
			termFreq = hit.getScore();
			docno = (String) hit.getSource().get("docno");
			docLength = hit.getSource().get("text").toString().split(" ").length;
			//System.out.println(docLength);
			double ls = Math.log(termFreq+1)/(V + docLength);
			if(!scores.containsKey(docno))
				scores.put(docno,ls);
			else
				scores.put(docno,scores.get(docno)+ls);
		}
		//System.out.println("okapihitsperword" + sumOfOkapiHitsPerWord);
	}
	

	public static void readQueries(File filename) throws IOException
	{
		String line = null;
		FileReader fileReader = new FileReader(filename);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		int i=0;
		while((line = bufferedReader.readLine()) != null) {
			String queryNo = cleaningQuery(line);
			HashSet wordsInQuery = queryMap.get(queryNo);
			HashMap<String,Double> docScores=new HashMap<String,Double>();
			getDocsForQuery(wordsInQuery,docScores,queryNo);
		}  
		bufferedReader.close();
	}
	
	public static void getDocsForQuery(HashSet<String> words,HashMap<String,Double> docScores,String q) throws IOException
	{
		for(String word : words)
		{
			SearchHits hits = getResponse(word);
			//okapiTFNew(hits,docScores);
			//TFIDFNew(hits,docScores);
			OkapiBM25(hits,docScores);
			//laplaceScore(hits, docScores);
		}
		sortandPrintScores(docScores,q);
		
		
	}
	
	public static void sortandPrintScores(Map<String, Double> scores,String id) throws IOException
	{
		List<Map.Entry<String, Double>> list = new LinkedList<Map.Entry<String, Double>>(scores.entrySet());

		Collections.sort(list,new Comparator<Map.Entry<String, Double>>(){
			public int compare(Map.Entry<String, Double> p1, Map.Entry<String, Double> p2) {
				return p2.getValue().compareTo(p1.getValue());
			}
		});
		
		writeOutput(list,id);
	}
	
	public static void writeOutput(List<Map.Entry<String, Double>> list, String id) throws IOException
	{
		int rank=0;
		PrintWriter out= new PrintWriter(new BufferedWriter(new FileWriter("okapiBM25.txt",true)));
		
		for(Map.Entry<String, Double> item:list)
		{
			String line=id + " Q0 " + item.getKey() + " " + (++rank) + " " + item.getValue() + " " + "Exp";
			//System.out.println(line);
			out.write(line+"\n");
			if(rank==1000)
				break;
		}
		out.close();
	}
		

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		File queryFile = new File("C://InformationRetrieval//AP89_DATA//AP_DATA//query_desc.51-100.short.txt");
		//File queryFile = new File("E://AP89_DATA//AP_DATA//query_desc.51-100.short.txt");
		readQueries(queryFile);
		System.out.println(Query.getResponse("male"));
		//getDocuments();
	}
}

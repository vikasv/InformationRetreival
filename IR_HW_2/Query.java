import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class Query {
	static HashMap<String, HashSet<String>> queryMap = new HashMap<String, HashSet<String>>();
	static Stemmer stmr = new Stemmer();
	static Map<String,HashMap<String,Integer>> invIndex=new HashMap<String,HashMap<String,Integer>>();
	static Map<String, Integer> tokenCount=new HashMap<String, Integer>();
	static HashSet<String> stopWords=new HashSet<String>();
	static int limit =0;
	static Double fullCount=0.0;
	final Double k1=1.2;
	final Double k2=100.0;
	final Double b=0.75;
	
	public static void stopWordsFile() throws IOException, InterruptedException
	{
		File stopWordFile = new File("C://Users//vikas//Documents//GitHub//InformationRetreival//InformationRetreival//IR_HW_2//stoplist.txt");
		BufferedReader br = new BufferedReader(new FileReader(stopWordFile));
		String line = null;
		while ((line = br.readLine()) != null) {
			stopWords.add(line.trim());
		}
	}
	
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
				word=stmr.stem(word.trim());
				if(stopWords.contains(word.trim()))
				{
					continue;
				}
				
				hs.add(word.trim());
				
			}
			queryMap.put(queryDivision[0],hs);
			queryNo = queryDivision[0];
		}
		return queryNo;	
	}

	public static void TFIDF(HashMap<String, Integer> index,HashMap<String,Double> scores)
	{
		double termFreq = 0.0;
		int docLength = 0;
		long totalDocLen = 84679;
		Double N=tokenCount.size()*1.0;
		double avgLengthDoc=fullCount/N;
		double dfw=index.size();
		for (String docID:index.keySet())
		{
			
			termFreq = index.get(docID);
			docLength = tokenCount.get(docID);
			double TFIDFperHit = termFreq/(termFreq + 0.5 + (1.5 * ((docLength)/(avgLengthDoc)))) * Math.log(totalDocLen/dfw);
			if(!scores.containsKey(docID))
				scores.put(docID,TFIDFperHit);
			else
				scores.put(docID,scores.get(docID)+TFIDFperHit);
		}
	}
	
	public static void OkapiBM25(HashMap<String, Integer> index,HashMap<String,Double> scores)
	{
		double termFreq = 0.0;
		int docLength = 0;
		long totalDocLen = 84679;
		Double N=tokenCount.size()*1.0;
		double avgLengthDoc=fullCount/N;
		double dfw=index.size();
		double k1 = 1.2;
		int k2 = 100;
		double b = 0.75;
		for (String docID:index.keySet())
		{

			termFreq = index.get(docID);
			docLength = tokenCount.get(docID);
			//System.out.println(docLength);
			double okapiBM25perHit = (termFreq+(k1*termFreq))/(termFreq+ (k1*((1-b)+(b*docLength/avgLengthDoc)))) * Math.log((totalDocLen+0.5)/(dfw+0.5));
			
			if(!scores.containsKey(docID))
				scores.put(docID,okapiBM25perHit);
			else
				scores.put(docID,scores.get(docID)+okapiBM25perHit);
		}
		
	}
	
	public static void laplaceScore(HashMap<String, Integer> index,HashMap<String,Double> scores)
	{		
		String queryId;
		double termFreq;
		String docno;
		double docLength;
		double sumOfOkapiHitsPerWord = 0.0 ;
		double V = 173377.0;
		
		for (String docID:index.keySet())
		{
			//System.out.println(hit.getId() + "-" + hit.getScore() + " - " + hit.getSource().get("docno")+" - "+hit.getSource().get("text").toString().length());

			termFreq = index.get(docID);
			docLength = tokenCount.get(docID);
			//System.out.println(docLength);
			double ls = Math.log(termFreq+1)/(V + docLength);
			if(!scores.containsKey(docID))
				scores.put(docID,ls);
			else
				scores.put(docID,scores.get(docID)+ls);
		}
		//System.out.println("okapihitsperword" + sumOfOkapiHitsPerWord);
	}
	
	public static void readIndex(String path)
	{
		FileInputStream fis=null;
		ObjectInputStream ois=null;
		try 
		{
			fis=new FileInputStream(path);
			ois=new ObjectInputStream(fis);
			invIndex=(HashMap<String,HashMap<String,Integer>>)ois.readObject();
			tokenCount=(HashMap<String, Integer>)ois.readObject();
			
			for(String docID:tokenCount.keySet())
				fullCount+=tokenCount.get(docID);
		} 
		catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		finally{try {ois.close();} catch (IOException e){e.printStackTrace();}}
	}
	

	public static void readQueries(File filename) throws IOException
	{
		String line = null;
		FileReader fileReader = new FileReader(filename);
		BufferedReader bufferedReader = new BufferedReader(fileReader);

		while((line = bufferedReader.readLine()) != null) {
			String queryNo = cleaningQuery(line);
			HashSet<String> wordsInQuery = queryMap.get(queryNo);
			HashMap<String,Double> docScores=new HashMap<String,Double>();
			getDocsForQuery(wordsInQuery,docScores,queryNo);
		}  
		bufferedReader.close();
	}
	
	public static void getDocsForQuery(HashSet<String> words,HashMap<String,Double> docScores,String q) throws IOException
	{
		for(String word : words)
		{
			HashMap<String, Integer> index = invIndex.get(word);
			
			try{
			//TFIDF(index,docScores);
			//OkapiBM25(index,docScores);
			laplaceScore(index, docScores);
			}
			catch(Exception ex)
			{
				System.out.println(word);
				break;
			}
			//laplaceScore(index, docScores);
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
	PrintWriter out= new PrintWriter(new BufferedWriter(new FileWriter("verification_laplace_StopAndStemIndex.txt",true)));
		
		for(Map.Entry<String, Double> item:list)
		{
			String line=id + " Q0 " + item.getKey() + " " + (++rank) + " " + item.getValue() + " " + "Exp";
			System.out.println(line);
			out.write(line+"\n");
			if(rank==1000)
				break;
		}
		out.close();
	}
		

	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		stopWordsFile();
		readIndex("C://Users//vikas//Documents//GitHub//InformationRetreival//InformationRetreival//IR_HW_2//Verification_TA.txt");
		File queryFile = new File("C://Users//vikas//Documents//GitHub//InformationRetreival//InformationRetreival//IR_HW_2//queries.txt");
		readQueries(queryFile);


	}
}

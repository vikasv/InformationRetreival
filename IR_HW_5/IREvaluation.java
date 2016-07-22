import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class IREvaluation {

	public static LinkedHashMap<String,Integer> BM25HashMap = new LinkedHashMap<String,Integer>();
	public static LinkedHashMap<String,Integer> RelevancyHashMap = new LinkedHashMap<String,Integer>();
	public static int relevantUrls = 0;
	public static HashSet<String> QueryIds = new HashSet<String>();

	public static void relevantUrls()
	{
		Map<String, Integer> map = RelevancyHashMap;
		for(Entry<String, Integer> entry : map.entrySet())
		{
			if(entry.getValue()==1 || entry.getValue()==2)
			{
				relevantUrls++;
			}
		}
	}

	public static double AveragePrecision()
	{

		Map<String, Integer> map = BM25HashMap;
		int count = 0;
		double avgPrecision = 0.0;
		for(Entry<String, Integer> entry : map.entrySet())
		{
			String urlKey = entry.getKey();
			if(RelevancyHashMap.containsKey(urlKey)){
				if((RelevancyHashMap.get(urlKey) == 1) || (RelevancyHashMap.get(urlKey) == 2))
				{
					count++;
					avgPrecision = avgPrecision +  ((double) count/ (double) entry.getValue());
				}
			}
		}
		return avgPrecision/relevantUrls;
	}

	public static double DCG()
	{

		LinkedHashMap<String, Integer> map = BM25HashMap;
		int count = 1;
		double dcg = 0.0;
		for(Entry<String, Integer> entry : map.entrySet())
		{
			String urlKey = entry.getKey();
			if(RelevancyHashMap.containsKey(urlKey)){
				if((RelevancyHashMap.get(urlKey) == 0) ||(RelevancyHashMap.get(urlKey) == 1) || (RelevancyHashMap.get(urlKey) == 2))
				{
					int relevance = RelevancyHashMap.get(urlKey);
					if(count == 1)
					{
						dcg = dcg + relevance;
					}
					else
					{
						double logCount = ((double) Math.log(count))/((double) Math.log(2));
						dcg = dcg + ((double) relevance /(double) logCount);
					}

					count++;
				}
			}
		}
		LinkedHashMap<String,Integer> SortedRelevancyHashMap = (LinkedHashMap<java.lang.String, java.lang.Integer>) sortByValue(RelevancyHashMap);
		double idealDcg = 0.0;
		int idealCount = 1;
		for(Entry<String, Integer> entry : map.entrySet())
		{
			String urlKey = entry.getKey();
			if(SortedRelevancyHashMap.containsKey(urlKey)){
				if((SortedRelevancyHashMap.get(urlKey) == 0) ||(SortedRelevancyHashMap.get(urlKey) == 1) || (SortedRelevancyHashMap.get(urlKey) == 2))
				{
					int relevance = SortedRelevancyHashMap.get(urlKey);
					if(idealCount == 1)
					{
						idealDcg = idealDcg + relevance;
					}
					else
					{
						double logCount = ((double) Math.log(idealCount))/((double) Math.log(2));
						idealDcg = idealDcg + ((double) relevance /(double) logCount);
					}
					idealCount++;
				}
			}
		}
		System.out.println("dcg value is : " + dcg);
		System.out.println("Ideal dcg value is : " + idealDcg);
		return dcg/idealDcg;
	}

	public static double newDCG()
	{
		LinkedHashMap<String, Integer> map = RelevancyHashMap;
		int count = 1;
		double dcg = 0.0;
		for(Entry<String, Integer> entry : map.entrySet())
		{
			String urlKey = entry.getKey();
			if(BM25HashMap.containsKey(urlKey)){
				int relevance = RelevancyHashMap.get(urlKey);
				if(count == 1)
				{
					dcg = dcg + relevance;
				}
				else
				{
					double logCount = ((double) Math.log(count))/((double) Math.log(2));
					dcg = dcg + ((double) relevance /(double) logCount);
				}
				count++;
			}
		}
		LinkedHashMap<String,Integer> SortedRelevancyHashMap = (LinkedHashMap<java.lang.String, java.lang.Integer>) sortByValue(RelevancyHashMap);
		LinkedHashMap<String, Integer> map1 = SortedRelevancyHashMap;
		double idealDcg = 0.0;
		int idealCount = 1;
		for(Entry<String, Integer> entry : map1.entrySet())
		{
			String urlKey = entry.getKey();
			if(SortedRelevancyHashMap.containsKey(urlKey)){
				int relevance = SortedRelevancyHashMap.get(urlKey);
				if(idealCount == 1)
				{
					idealDcg = idealDcg + relevance;
				}
				else
				{
					double logCount = ((double) Math.log(idealCount))/((double) Math.log(2));
					idealDcg = idealDcg + ((double) relevance /(double) logCount);
				}
				idealCount++;
			}
		}
		System.out.println("new dcg value is : " + dcg);
		System.out.println("new Ideal dcg value is : " + idealDcg);
		return dcg/idealDcg;
	}

	public static LinkedHashMap<String, Integer> sortMap(LinkedHashMap<String,Integer> map)
	{
		List<Entry<String,Integer>> list = new LinkedList<>(map.entrySet());
		Collections.sort(list, new Comparator<Object>() {
			@SuppressWarnings("unchecked")
			public int compare(Object o1, Object o2) {
				return ((Comparable<Integer>) ((Map.Entry<String,Integer>) (o2)).getValue()).compareTo(((Map.Entry<String,Integer>) (o1)).getValue());
			}
		});
		LinkedHashMap<String,Integer> sortedMap = new LinkedHashMap<>();
		for (Iterator<Entry<String,Integer>> it = list.iterator(); it.hasNext();) {
			Map.Entry<String,Integer> entry = (Map.Entry<String,Integer>) it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	public static <String, Integer extends Comparable<? super Integer>> Map<String, Integer> 
	sortByValue( Map<String, Integer> map )
	{
		List<Map.Entry<String, Integer>> list =
				new LinkedList<Map.Entry<String, Integer>>( map.entrySet() );
		Collections.sort( list, new Comparator<Map.Entry<String, Integer>>()
		{
			public int compare( Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2 )
			{
				return (o2.getValue()).compareTo(o1.getValue() );
			}
		} );

		Map<String, Integer> result = new LinkedHashMap<String, Integer>();
		for (Map.Entry<String, Integer> entry : list)
		{
			result.put( entry.getKey(), entry.getValue() );
		}
		return result;
	}

	public static double RPrecision()
	{
		double precision = precisionRecall(relevantUrls);
		//System.out.println("R precision for a given query is : " + precision);
		return precision;
	}

	public static double precisionRecall(int value)
	{
		Map<String, Integer> map = BM25HashMap;
		int count = 0;
		int pCount = 0;
		double precision = 0.0;
		double recall = 0.0;
		double fMeasure = 0.0;
		for(Entry<String, Integer> entry : map.entrySet())
		{
			count++;
			String urlKey = entry.getKey();
			if(count<=value && RelevancyHashMap.containsKey(urlKey)){
				if((RelevancyHashMap.get(urlKey) == 1) || (RelevancyHashMap.get(urlKey) == 2))
				{
					pCount++;
				}
			}

		}
		precision = (double) pCount/ (double) value;
		recall = (double) pCount/ relevantUrls;
		fMeasure = (2 * precision * recall)/(precision + recall);
		System.out.print("Precision at "+ value +" docs is : " + precision + " ");
		System.out.print("recall at "+ value +" docs is : " + recall + " ");
		System.out.println("fmeasure at "+ value +" docs is : " + fMeasure);
		return precision;

	}
	
	private static double calculateAverage(List <Double> rp) {
		  Double sum = 0.0;
		  if(!rp.isEmpty()) {
		    for (Double mark : rp) {
		        sum += mark;
		    }
		    
		    
		  }
		  return sum/rp.size();
		}

	public static void main(String[] args) throws IOException {
		List<Double> avgPrecList = new ArrayList<Double>();
		List<Double> rPrecList = new ArrayList<Double>();
		BufferedReader  BM25fileQueryIds = new BufferedReader(new FileReader("okapiBM25.txt"));
		String queryLine ="";
		while((queryLine = BM25fileQueryIds.readLine()) != null)
		{
			String delims = "\\s+";
			String id = queryLine.split(delims)[0];
			QueryIds.add(id);
		}
		for(String s: QueryIds)
		{
			String BMline = "";
			System.out.println("Query id : " + s);
			BufferedReader  BM25file = new BufferedReader(new FileReader("okapiBM25.txt"));
			while((BMline = BM25file.readLine()) != null)
			{
				String delims = "\\s+";
				String url = BMline.split(delims)[2];
				String id = BMline.split(delims)[0];
				int rank = Integer.parseInt((BMline.split(delims)[3]));
				if(s.equals(id))
				{
					BM25HashMap.put(url, rank);
				}
			}
			BufferedReader  RelevancyFile = new BufferedReader(new FileReader("qrel.txt"));
			String rline="";
			while((rline = RelevancyFile.readLine()) != null)
			{
				String delims = "\\s+";
				String url = rline.split(delims)[2]; //String url = BMline.split(" ")[1];
				String id = rline.split(delims)[0];
				int score = Integer.parseInt((rline.split(delims)[3])); //int score = Integer.parseInt((BMline.split(" ")[3]));
				if(s.equals(id))
				{
					RelevancyHashMap.put(url, score);
				}
			}
			relevantUrls();
			System.out.println("No of relevant urls for given query are " + relevantUrls);
			precisionRecall(5);
			precisionRecall(10);
			precisionRecall(20);
			precisionRecall(50);
			precisionRecall(100);
			precisionRecall(1000);
			System.out.println("Average precision for a given query is : " + AveragePrecision());
			avgPrecList.add(AveragePrecision());
			System.out.println("DCG for a given query is : " + DCG());
			System.out.println("new DCG for a given query is : " + newDCG());
			System.out.println("R precision for a given query is : " + RPrecision());
			rPrecList.add(RPrecision());
			System.out.println("");
			BM25HashMap.clear();
			RelevancyHashMap.clear();
			relevantUrls = 0;	
		}
		System.out.println("Average precision for all queries " + calculateAverage(avgPrecList));
		System.out.println("R precision for all queries " + calculateAverage(rPrecList));
		
	}

}

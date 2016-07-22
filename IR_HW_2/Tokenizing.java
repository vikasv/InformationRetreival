
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Tokenizing {
	
	
	static Long id = (long) 0;
	static Long termId = (long) 0;
	static HashMap<Long, HashMap<String,StringBuilder>> hm = new HashMap<Long, HashMap<String,StringBuilder>>();
	static HashMap<Long, String> hm2 = new HashMap<Long, String>(); 
	static HashMap<String,StringBuilder> DocNoDocText = new HashMap<String,StringBuilder>(); 
	static ArrayList<Triplet> tuples = new ArrayList<Triplet>();
	static HashMap<String,StringBuilder> TempHM; 
	static HashSet<String> stopWords=new HashSet<String>();
	static Map<String,HashMap<String,Integer>> invIndex=new HashMap<String,HashMap<String,Integer>>();
	static Map<String, Integer> tokenCount=new HashMap<String, Integer>();
	static Stemmer stmr=new Stemmer();
	
	public static void stopWordsFile() throws IOException, InterruptedException
	{
		File stopWordFile = new File("C://Users//vikas//Documents//GitHub//InformationRetreival//InformationRetreival//IR_HW_2//stoplist.txt");
		BufferedReader br = new BufferedReader(new FileReader(stopWordFile));
		String line = null;
		while ((line = br.readLine()) != null) {
			stopWords.add(line.trim());
		}
	}
	
	public static void processFile(File file) throws IOException, InterruptedException
	{
		BufferedReader br = new BufferedReader(new FileReader(file)); 
		
		String line = null;
		String DocNo = null;
		StringBuilder contentbwText=null;
		while ((line = br.readLine()) != null) {
			
			contentbwText=new StringBuilder();
			if (line.startsWith("<DOCNO>"))
			{
				DocNo = line.substring(7, line.length() - 8).trim();	
			}
			
			else if (line.startsWith("<TEXT>"))
			{
				while (!(line = br.readLine()).equals("</TEXT>"))
				{
					contentbwText.append(line+" ");
				}
			}
				
			if (DocNoDocText.containsKey(DocNo)){
				DocNoDocText.get(DocNo).append(" "+contentbwText);
			}
			else
			{
				DocNoDocText.put(DocNo,contentbwText);
			}
		}
		br.close();
	}

	private static void tokenize(String docID, String text) {

		String pattern = "(\\w+(\\.?\\w+)*)";
		Pattern pat = Pattern.compile(pattern);
		Matcher tokens = pat.matcher(text.toString().toLowerCase());
		int count=0;
		while(tokens.find())
		{
			String word=tokens.group();
			//In case of stopping - Uncomment the below if condition
			if(stopWords.contains(word.trim()))
			{
				continue;
			}
			//In case of stemming - Uncomment the below line
			word=stmr.stem(word);
			HashMap<String, Integer> indexes;
			if(invIndex.containsKey(word))
			{
				indexes=invIndex.get(word);
				if(indexes.containsKey(docID))
					indexes.put(docID, (Integer)indexes.get(docID) + 1);
				else
					indexes.put(docID, 1);
			}
			else
			{
				indexes=new HashMap<String, Integer>();
				indexes.put(docID,1);
			}
			invIndex.put(word, indexes);
			count++;
		}
		
		if(tokenCount.containsKey(docID))
			tokenCount.put(docID, tokenCount.get(docID) + count);
		else
			tokenCount.put(docID, count);
	}

	public static void writeFile(String fileName)
	{
		FileOutputStream fos=null;
		ObjectOutputStream oos=null;
		try 
		{
			File file = new File(fileName);
			if (!file.exists()) {
				file.createNewFile();
			}
			fos=new FileOutputStream(file);
			oos=new ObjectOutputStream(fos);
			oos.writeObject(invIndex);
			oos.writeObject(tokenCount);
		}
		catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {fos.close();oos.close(); } catch (IOException ignore) {}
		}
	}

	

	public static void main(String[] args) throws IOException
	{
		long startTime = System.currentTimeMillis();
		
		//System.out.println(startTime);		
		File file = null;
		File[] paths;
		try{
			file = new File("C://InformationRetrieval//AP89_DATA//AP_DATA//ap89_collection");
			paths = file.listFiles();
			for (File path:paths)
			{
				processFile(path);
			}
			System.out.println("Doc count: "+DocNoDocText.size()+"\nStarted Indexing");
			stopWordsFile();
			int d=0;
			for(String docNumber:DocNoDocText.keySet())
			{
				System.out.println("done: " + (++d));
				tokenize(docNumber,DocNoDocText.get(docNumber).toString());
			}
			
			System.out.println(invIndex.size());
			writeFile("Verification_TA.txt");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		long stopTime = System.currentTimeMillis();
	    long elapsedTime = stopTime - startTime;
	    System.out.println(elapsedTime);
	}

}

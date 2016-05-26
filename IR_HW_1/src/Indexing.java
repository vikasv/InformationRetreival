import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;

public class Indexing {
	
	static int id = 0;
	public static void processFile(File file) throws IOException, InterruptedException
	{
		BufferedReader br = new BufferedReader(new FileReader(file));
		HashMap<String, StringBuilder> hm = new HashMap<String, StringBuilder>();
		String line = null;
		String DocNo = null;
		// id will be integers
		// if Docno already present text should be combined
		while ((line = br.readLine()) != null) {
			if (line.startsWith("<DOCNO>"))
			{
				DocNo = line.substring(7, line.length() - 8).trim();
				if (!(hm.containsKey(DocNo)))
				{
					id++;
				}		
			}
			
			if (line.startsWith("<TEXT>"))
			{
				StringBuilder contentbwText = new StringBuilder();
				while (!(line = br.readLine()).equals("</TEXT>"))
				{
					contentbwText.append(" ");
					contentbwText.append(line);
				}	
				if ((hm.containsKey(DocNo)))
				{
					contentbwText.append(" ");
					contentbwText.append(hm.get(DocNo));
				}			
				hm.put(DocNo, contentbwText);
	
		        URL url = new URL("http://localhost:9200/ap_dataset/document/" + id);
		        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
		        connection.setRequestProperty("Accept", "application/json");
		        connection.setRequestMethod("PUT");
		        connection.setDoOutput(true);
//		        connection.setRequestProperty("Content-Type", "application/json");
//		        connection.setRequestProperty("Accept", "application/json");
		        OutputStreamWriter osw = new OutputStreamWriter(connection.getOutputStream());
		        osw.write(" { \"text\": \"" + contentbwText + "\"" + "," + "\"docno\": \"" + DocNo + "\" }");
		        osw.flush();
		        osw.close();
		        connection.disconnect();
		        System.err.println(connection.getResponseCode());
//				System.out.println(contentbwText);
//		        System.out.println(DocNo);
			}	
		}
		Thread.sleep(1000);
		br.close();
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

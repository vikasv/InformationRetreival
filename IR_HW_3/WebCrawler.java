import java.io.*;
import java.net.*;
import java.util.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class WebCrawler {

	List<String> visited = new ArrayList<String>();
	List<Site> toBeVisited = new ArrayList<>();
	List<String> toBeVisitedUrl = new ArrayList<>();
	HashMap<String, Site> frontier = new HashMap<>();
	List<String> robotsDomainList = new ArrayList<>();
	HashMap<String,Long> domainTime = new HashMap<>(); 
	HashMap disallowListCache=new HashMap();
	List<String> relevantTerms=new ArrayList<String>();
	HashMap<String,Integer> urlFileName = new HashMap<>();
	int maxDocs;


	public static String canonicalDomain(URL url) {
		int port = url.getPort();
		if ((url.getProtocol().equals("http") && port == 80) || (url.getProtocol().equals("https") && port == 443)) {
			port = -1;
		}
		return (url.getHost() + (port == -1 ? "" : ":" + port)).toLowerCase();
	}

	private  boolean RobotFiles(String url) throws Exception 
	{
		URL urlToCheck = new URL(url);

		String host = urlToCheck.getHost().toLowerCase();
		String protocol = urlToCheck.getProtocol();

		ArrayList disallowList = (ArrayList) disallowListCache.get(host);

		//If list is not in the cache, download and cache it.
		if (disallowList == null) 
		{
			disallowList = new ArrayList();
			try {				

				//	System.out.println("robotsDomainList: "+robotsDomainList);

				if(robotsDomainList.contains(host))
				{
					ArrayList list = (ArrayList)disallowListCache.get(host);
					if(list.contains(urlToCheck.getPath()))	
						return false;
					else
						return true;
				}
				else
				{
					URL robotsFileUrl = new URL(protocol+"://" + host + "/robots.txt");
					URLConnection urlConn = robotsFileUrl.openConnection();
					urlConn.setConnectTimeout(5000);
					urlConn.setReadTimeout(5000);
					urlConn.setAllowUserInteraction(false);         
					urlConn.setDoOutput(true);

					robotsDomainList.add(host);

					File roboFile = new File("roboFiles\\"+host);

					if(!roboFile.exists())
						roboFile.createNewFile();

					// System.out.println("robofile created at :"+roboFile.getAbsolutePath());
					BufferedWriter writer = new BufferedWriter(new FileWriter(roboFile));
					BufferedReader reader =	new BufferedReader(new InputStreamReader(urlConn.getInputStream()));
					String line;
					while ((line = reader.readLine()) != null) 
					{
						if(line.trim().toLowerCase().equals("user-agent: *"))
						{	
							while(true)
							{
								line = reader.readLine();

								if(line==null)
									break;
								else if (line.toLowerCase().startsWith("user")) {
									break;
								}
								else
								{
									if (line.startsWith("Disallow:")) 
									{
										writer.write(line);
										String disallowPath = line.substring("Disallow:".length()).trim();

										//Add disallow path to list.
										disallowList.add(disallowPath);
									}
								}
							}
						}
						else
							continue;
					}
					reader.close();
					writer.close();
				}


				//Add new disallow list to cache.
				disallowListCache.put(host, disallowList);
			}
			catch (Exception e) {
				System.out.println("Exception occured while accessing robots.txt file - Exception ignored");
				return true;
			}
		}

		/* Loop through disallow list to see if crawling is allowed for the given URL. */
		String file = urlToCheck.getFile();
		for (int i = 0; i < disallowList.size(); i++)
		{
			String disallow = (String) disallowList.get(i);

			if (file.startsWith(disallow))
				return false;
		}

		return true;
	}

	public String cannonical_url(String urlStr) throws MalformedURLException, UnsupportedEncodingException
	{
		String curDomain = "";
		String path = "";
		String final_return="";
		if(urlStr.contains("http://") || urlStr.contains("https://"))
		{
			URL url = new URL(urlStr);
			curDomain = canonicalDomain(url);
			path = url.getPath();
			if (path.equals("/") || path.equals("")) {
				final_return= curDomain;
			} else {
				urlStr = urlStr.replaceAll("%(?![0-9a-fA-F]{2})", "%25");
				urlStr = urlStr.replaceAll("\\+", "%2B");
				String decoded = URLDecoder.decode(urlStr, "UTF-8");
				URL decodedurl = new URL(decoded);
				String decodedpath = decodedurl.getPath().toLowerCase();
				String[] new_delims = decodedpath.split("#");
				decodedpath = new_delims[0];
				String[] delims = decodedpath.split("/");
				String new_path = "";
				Integer len = delims.length;
				for (String s : delims) {
					if (s.contains("index.htm") || s.contains("index.html") || s.contains("index.asp")
							|| s.contains("index.aspx")) {

						new_path = new_path + "";
						break;
					} else if (new_path == "") {
						new_path = s;
					} else
						new_path = new_path + "/" + s;
				}

				if (!delims[len - 1].contains(".htm") || !delims[len - 1].contains(".html")) {
					new_path = new_path + "/";
				}
				final_return= (decodedurl.getHost() + new_path).toLowerCase();
			}
		} return final_return;
	}
	
	//Checks each of the given links for Disallow list in robots.txt and returns only allowed links
	public List<Site> getRobotsAllowedLinks(List<Site> links) throws Exception
	{
		List<Site> validURLs=new ArrayList<Site>();

		for(Site link : links)
		{
			if(RobotFiles(link.url))
			{
				validURLs.add(link);
			}
			else
			{
				System.out.println("disallowed Link found");
			}
		}
		return validURLs;
	}

	/*public void cleanUpHtmlDocs() throws IOException
	{
		File f = new File("htmlDocs\\");		
		File[] htmlDocs = f.listFiles();
		for(int i=0;i<htmlDocs.length;i++)
		{
			htmlDocs[i].delete();
		}
		System.out.println("all html Docs deleted");
	}*/
	
	public void cleanUpRoboFiles() throws IOException
	{
		File f = new File("roboFiles\\");		
		File[] roboFiles = f.listFiles();
		for(int i=0;i<roboFiles.length;i++)
		{
			roboFiles[i].delete();
		}
		System.out.println("all roboFiles deleted");
	}

	
	//Keeps track of the timestamp for each domain and slows down the request for a second if the previous request to the domain 
	//was less than a second
	public void politeness(Site s) throws InterruptedException
	{
		if(domainTime.containsKey(s.domain))
		{
			if((System.currentTimeMillis() - domainTime.get(s.domain))<1000.0)
			{
				System.out.println("Same domain: Sleeping for 1 sec");
				Thread.sleep(1000);
			}
		}
		else
		{
			domainTime.put(s.domain, System.currentTimeMillis());
		}
	}


	//Sort the outlinks for a given site based on their inLinkscount and return just the first half in the sorted list
	public List<Site> prioritizeOutLinks(List<Site> outlinks)
	{
		Collections.sort(outlinks,new SiteInLinksComparator());
		outlinks = outlinks.subList(0, outlinks.size()/3);
		return outlinks;
	}

	
	//Calculates the relevancy of a given document with a relevancy file created with relevant words
	public int getRelevance(Document doc) throws FileNotFoundException
	{
		int relevance =1;

		String delims[]=doc.body().text().split(" ");
		for (String sd: delims)
		{
			sd = sd.toLowerCase();
			for (String ds:relevantTerms)
			{
				if(Objects.equals(ds,sd))
					relevance++;
			}
		}

		return relevance;
	}

	
	//calculate relevacy for all the outlinks of a site
	public List<Site> calculateRelevancy(List<Site> outlinks) throws IOException
	{

		int count = 0;
		for(Site link: outlinks)
		{

			System.out.println("getting html doc for child: "+count++);
			URLConnection urlConn = getConnection(link.url);
			Document doc = getUrlHtmlDoc(link.url,urlConn);
			if(doc==null)
				continue;

			link.relevancy = getRelevance(doc);
		}

		return outlinks;
	}


	//Sorts and returns the outlinks based on their relevancy score
	public List<Site> prioritizeByRelevancy(List<Site> outlinks)
	{
		Collections.sort(outlinks,new SiteRelevancyComparator());
		return outlinks;
	}

	//Download raw HTML file for a given url
	public File downloadRawFile(URLConnection urlConn,int fileNo) throws IOException
	{
		File f = new File("htmlDocs\\"+fileNo+"");
		BufferedReader br = new BufferedReader(new InputStreamReader(urlConn.getInputStream()));

		BufferedWriter bw = new BufferedWriter(new FileWriter(f));

		String line="";

		while((line=br.readLine())!=null)
		{
			bw.write(line);
		}

		bw.close();
		br.close();

		return f;
	}

	//Get connection for a given url
	public URLConnection getConnection(String url) throws IOException
	{
		URL tempUrl = new URL(url);
		URLConnection urlConn = tempUrl.openConnection();
		urlConn.setConnectTimeout(5000);
		urlConn.setReadTimeout(5000);
		urlConn.setAllowUserInteraction(false);         
		urlConn.setDoOutput(true);

		return urlConn;
	}

	//get headers for a given url
	public Map<String, List<String>> getHeaders(URLConnection urlConn)
	{
		Map<String, List<String>> map = urlConn.getHeaderFields();
		return map;
	}

	
	//Downloads a html raw file if it's not already download or returns the existing htmml document if it's downloaded
	public Document getUrlHtmlDoc(String url,URLConnection urlConn) throws IOException
	{
		try
		{
			if(urlFileName.containsKey(url))
			{
				File f = new File("htmlDocs\\"+urlFileName.get(url)+"");
				return Jsoup.parse(f, "UTF-8");
			}
			else
			{
				if(urlConn.getContentType().contains("text/html"))  //accept only if the content type is text/html
				{
					if(urlFileName.isEmpty())
						urlFileName.put(url, 1);
					else
						urlFileName.put(url, urlFileName.size()+1);

					File f = new File("htmlDocs\\"+urlFileName.get(url)+"");
					f=downloadRawFile(urlConn,urlFileName.get(url)); 
					Document htmlDocument = Jsoup.parse(f, "UTF-8");
					return htmlDocument;
				}
				else
					return null;
			}
		}
		catch(Exception e)
		{
			System.out.println("Exception while getUrlHtml doc for URL :"+url);
			System.out.println("exception ignored");
			//e.printStackTrace();
			return null;
		}
	}

	//Write each sites information to a an index file
	public void writeToFile(Site s) throws IOException
	{
		File f = new File("FinalFile.txt");

		if(!f.exists())
			f.createNewFile();

		BufferedWriter bw = new BufferedWriter(new FileWriter(f,true));
		bw.write("<Site>");
		bw.newLine();
		bw.write("<URL>");
		bw.write(s.url);
		bw.write("</URL>");
		bw.newLine();
		bw.write("<CannonicalURL>");
		bw.write(s.cannonicalUrl);
		bw.write("</CannonicalURL>");
		bw.newLine();
		bw.write("<Level>");
		bw.write(s.level+"");
		bw.write("</Level>");
		bw.newLine();
		bw.write("<Headers>");
		for(String key: s.headers.keySet())
		{
			if(!(key==null))
				bw.write(key+" : ");
			for(String item : s.headers.get(key))
			{
				bw.write(item);
			}
		}				
		bw.write("</Headers>");
		bw.newLine();
		bw.write("<Title>");
		bw.write(s.title);
		bw.write("</Title>");
		bw.newLine();
		bw.write("<FileNo>");
		bw.write(urlFileName.get(s.url)+"");
		bw.write("</FileNo>");
		bw.newLine();
		bw.write("<InLinkURLs>");
		for(String link : s.inLinkUrls)
			bw.write(link+",");
		bw.write("</InLinkURLs>");
		bw.newLine();
		bw.write("<OutLinkURLs>");
		for(String link : s.outLinkUrls)
			bw.write(link+",");
		bw.write("</OutLinkURLs>");
		bw.newLine();
		bw.write("<Author>");
		bw.write(s.author);
		bw.write("</Author>");
		bw.newLine();
		bw.write("</Site>");
		bw.newLine();

		bw.close();

	}

	//displays a site information
	public void displaySite(Site s)
	{
		System.out.println("Site info: ");
		System.out.println("url : "+s.url);
		System.out.println("canonical_url: "+s.cannonicalUrl);
		System.out.println("domain: "+s.domain);
		System.out.println("inlinks count: "+s.inLinksCount);
		System.out.println("inlinks:"+s.inLinkUrls);
		System.out.println("outlinks: "+s.outLinkUrls);
		System.out.println("author: "+s.author);
		System.out.println("title: "+s.title);
	}

	//returns the level of a site that  is next in the toBeVisited queue
	public int getNextLevel(String url) throws MalformedURLException, UnsupportedEncodingException
	{
		//remove the first site in the queue and add it to the visited queue
		toBeVisited.remove(0);
		visited.add(cannonical_url(url));

		//return level of next node in queue
		int nextLevel = -1;   //No more levels
		if(!toBeVisited.isEmpty())
			nextLevel= toBeVisited.get(0).level;

		return nextLevel;
	}
	

	//Crawl a given site
	public int crawl(Site s) throws Exception
	{
		long startTime = System.currentTimeMillis();
		
		String url=s.url;
		int level=s.level;

		System.out.println("crawling site: "+url+"\t @level: "+level);

		try
		{
			//check domain time stamp
			politeness(s);

			//fetching all child links
			URLConnection urlConn = getConnection(url);
			Document doc = getUrlHtmlDoc(url,urlConn);

			s.headers = getHeaders(urlConn);
		
			if(doc==null)
			{
				toBeVisited.remove(0);

				if(!toBeVisited.isEmpty())
					return toBeVisited.get(0).level;

				else
					return -200; //only when document is null
			}

			Elements linksOnPage = doc.select("a[href]");
			s.title= doc.title();

			System.out.println("Found (" + linksOnPage.size() + ") links");

			List<Site> outLinkSites = new ArrayList<>();

			for(Element link : linksOnPage)
			{
				Site ste= new Site();
				
				if(link.attr("href").startsWith("#"))
					continue;

				String absUrl =link.attr("href");
				if(absUrl.startsWith("//"))
					absUrl="https:"+absUrl;
				else if (absUrl.startsWith("https")|| absUrl.startsWith("http")) {
					absUrl = absUrl;
				}
				else
					absUrl="https://"+s.domain+absUrl;

				ste.url = absUrl;

				try
				{
					URL temprl = new URL(ste.url);
					ste.domain = temprl.getHost();
				}
				catch(Exception e)
				{
					//e.printStackTrace();
					System.out.println("ignoring the error while creating URL for link : "+ste.url);
					continue;
				}
				
				ste.level= level+1;
				ste.inLinksCount=ste.inLinksCount+1;
				if(!ste.inLinkUrls.contains(url))
					ste.inLinkUrls.add(url);

				ste.cannonicalUrl = cannonical_url(ste.url);

				if(!s.outLinkUrls.contains(ste.cannonicalUrl))
					outLinkSites.add(ste);					

			}
			linksOnPage=null;

			//avoid outlinks processing for a site if the toBeVisited hits the max capacity
			if(toBeVisited.size()<this.maxDocs)
			{
				//Prioritize based on InLinks and take first 50%
				outLinkSites = prioritizeOutLinks(outLinkSites);
				outLinkSites = calculateRelevancy(outLinkSites);
				outLinkSites = prioritizeByRelevancy(outLinkSites);
				outLinkSites = getRobotsAllowedLinks(outLinkSites);

				//System.out.println("after removing disallow links given in robots.txt file : "+outLinkSites.size());
			}
			else
				System.out.println("Ignoring extra processing for child links because ToBeVisited crossed maxDocs limit");


			for(Site outlinkSite: outLinkSites)
			{
				if(!s.outLinkUrls.contains(outlinkSite.cannonicalUrl))
					s.outLinkUrls.add(outlinkSite.cannonicalUrl);

				
				//add to the toBeVisited queue
				if(!visited.contains(outlinkSite.cannonicalUrl) && !toBeVisitedUrl.contains(outlinkSite.url))
				{
					toBeVisited.add(outlinkSite);
					toBeVisitedUrl.add(outlinkSite.url);
				}
				else
					continue;
			}

			System.out.println("Current size of toBeVisited queue: "+toBeVisited.size());
			
			s.outLinkCount=outLinkSites.size();

			int nextLevel = getNextLevel(url);

			System.out.println("Time taken for this crawl: "+(System.currentTimeMillis() - startTime)/1000.0 +"sec(s)");
			//displaySite(s);
			writeToFile(s);

			return nextLevel;
		}
		catch(Exception e)
		{
			//e.printStackTrace();
			System.out.println("exception caught in Crawl method");
			return getNextLevel(url);	//exception
		}

	}

	
	//Sort the toBeVisited queue based on InLinksCount and relevancy score after each level/wave
	public void prioritizeAfterLevel(int level) throws InterruptedException
	{
		Collections.sort(toBeVisited,new SiteOutLinksComparator());
		Collections.sort(toBeVisited,new SiteRelevancyComparator());
	}

	//read relevancy file and upadte the relevance hashmap
	public void getRelevantTerms(String fileName)
	{
		try
		{
			File file=new File(fileName);
			if(!file.exists())
				System.out.println("where is the file?");
			Scanner s=new Scanner(file); 
			while(s.hasNext())
			{
				relevantTerms.add(s.next().toLowerCase());
			}

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	//Crawl each seed URL
	public void crawlFromSeed(List<String> seeds,int maxDocs,String relevancyFileName) throws Exception
	{

		toBeVisited= new ArrayList<>();
		toBeVisitedUrl= new ArrayList<>();
		visited= new ArrayList<>();

		getRelevantTerms(relevancyFileName);
		int count=0;
		int level =0;
		for(String seedUrl : seeds)
		{
			Site s = new Site();

			s.url = seedUrl;
			s.level = level;
			
			URL tempUrl = new URL(seedUrl);
			s.domain = tempUrl.getHost();
			s.cannonicalUrl = cannonical_url(seedUrl);

			//adding seed sit to the toBeVisited Queue
			toBeVisited.add(s);

		}
		
		this.maxDocs = maxDocs;

		//frontier.put(cannonical_url(nextUrl), s);

		while(!toBeVisited.isEmpty() && count< maxDocs)
		{
			Site stemp = toBeVisited.get(0);
			int nextLevel=crawl(stemp);			
			if(nextLevel == -200)
			{
				System.out.println("Error during parsing HTML file. Ignoring error");
				continue;
			}
			
			if(nextLevel <0)
				break;

			if(stemp.level!=nextLevel)
				prioritizeAfterLevel(stemp.level);

			count++;
			System.out.println("No of sites crawled so far : "+count);
		}

	}

	public static void main(String args[]) throws Exception
	{

		String seedUrl1="https://en.wikipedia.org/wiki/Data_structure";
		String seedUrl2="http://en.wikipedia.org/wiki/Associative_array";
		String seedUrl3="http://en.wikipedia.org/wiki/Hash_table";
		
		List<String> seeds = new ArrayList<String>();
		seeds.add(seedUrl1);
		seeds.add(seedUrl2);
		seeds.add(seedUrl3);
		
		WebCrawler spider1 = new WebCrawler();
		spider1.cleanUpRoboFiles();
		//spider1.cleanUpHtmlDocs();
		
		spider1.crawlFromSeed(seeds,30000,"RelevanceWords.txt");
		
		System.out.println("crawling for the given URLs completed succesfully!");
		spider1.cleanUpRoboFiles();
		
	}
}
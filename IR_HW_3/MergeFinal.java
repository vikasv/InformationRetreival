import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.jsoup.Jsoup;

public class MergeFinal {

	public static String getFileText(String filename,String title) throws IOException
	{
		String text="";

		for(int i=2;i<3;i++)
		{
			File f = new File("htmlDocs\\"+filename);

			String docTitle = Jsoup.parse(f,"UTF-8").title();
			if(title.equals(docTitle))
				return Jsoup.parse(f,"UTF-8").toString();
			else
				System.out.println("Matching title not found");
		}
		return text;
	}

	public static void main(String[] args) throws IOException, InterruptedException
	{

		Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
				.put("cluster.name","mvk_cluster").build();

		Client client = TransportClient.builder().settings(settings).build()
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
		BulkRequestBuilder breq=client.prepareBulk(); 

		String content = new String(Files.readAllBytes(Paths.get("FinalFile.txt")));
		
		Pattern p = Pattern.compile("<Site>\\s*(.+?)\\s*</Site>", Pattern.DOTALL);
		Matcher m = p.matcher(content);
		String my_site="";
		int count =0;
		while (m.find())
		{
			count++;
			my_site=m.group(1);
			
			Pattern p1 = Pattern.compile("<URL>(.*)</URL>");
			Matcher m1 = p1.matcher(my_site);
			m1.find();
			String url=m1.group(1);
			System.out.println("count : "+count+" parsing Site: "+url);
			Pattern p2 = Pattern.compile("<CannonicalURL>(.*)</CannonicalURL>", Pattern.DOTALL);
			Matcher m2 = p2.matcher(my_site);
			m2.find();
			String cannonical_url=m2.group(1);

			Pattern p3 = Pattern.compile("<Level>(.*)</Level>", Pattern.DOTALL);
			Matcher m3 = p3.matcher(my_site);
			m3.find();
			String depth=m3.group(1);

			Pattern p4 = Pattern.compile("<Headers>(.*)</Headers>", Pattern.DOTALL);
			Matcher m4 = p4.matcher(my_site);
			m4.find();
			String HTTPheader=m4.group(1);

			Pattern p5 = Pattern.compile("<Title>(.*)</Title>", Pattern.DOTALL);
			Matcher m5 = p5.matcher(my_site);
			m5.find();
			String title=m5.group(1);

			Pattern p6 = Pattern.compile("<FileNo>(.*)</FileNo>", Pattern.DOTALL);
			Matcher m6 = p6.matcher(my_site);
			m6.find();
			String fileno=m6.group(1);
			String htmlDoc = getFileText(fileno,title);
			String htmlText = Jsoup.parse(htmlDoc).body().text();

			Pattern p7 = Pattern.compile("<InLinkURLs>(.*)</InLinkURLs>", Pattern.DOTALL);
			Matcher m7 = p7.matcher(my_site);
			m7.find();
			String in_links=m7.group(1);

			Pattern p8 = Pattern.compile("<OutLinkURLs>(.*)</OutLinkURLs>", Pattern.DOTALL);
			Matcher m8 = p8.matcher(my_site);
			m8.find();
			String out_links=m8.group(1);

			Pattern p9 = Pattern.compile("<Author>(.*)</Author>", Pattern.DOTALL);
			Matcher m9 = p9.matcher(my_site);
			m9.find();
			String author=m9.group(1);
			System.out.println("author(s) from file: "+author);
			
		//	String merged_url= new String();
/*			String merged_header= new String();
			String merged_title= new String();
			String merged_text= new String();
			String merged_htmlsource= new String();
*/			String merged_inlinks= new String();
			String merged_outlinks= new String();
			//String merged_author= new String();
			String merged_depth= new String();
			List<Object> merged_author_list = new ArrayList<Object>();
			
			
			/*GetResponse response = client.prepareGet("twitter", "tweet", "1")
			        .execute()
			        .actionGet();*/
			
			
			SearchResponse response = client.prepareSearch("final_site")
					.setTypes("document") 
					.setQuery(QueryBuilders.termQuery("_id",cannonical_url))
					.addScriptField("getheader", (new Script("doc['HTTPheader'].value")))
					.addScriptField("gettitle", (new Script("doc['title'].values")))
					.addScriptField("gettext", (new Script("doc['text'].value")))
					.addScriptField("gethtmlsource", (new Script("doc['html_source'].value")))
					.addScriptField("getinlinks", (new Script("doc['in_links'].value")))
					.addScriptField("getoutlinks", (new Script("doc['out_links'].value")))
					.addScriptField("getauthors", (new Script("doc['author'].values")))
					.addScriptField("getdepth", (new Script("doc['depth'].value")))
					.addScriptField("geturl", (new Script("doc['url'].value")))
					.execute().actionGet();
			
			
			long hits=response.getHits().getTotalHits();
			//System.out.println("hits "+hits);

			if(hits==0)
			{
				breq.add(client.prepareIndex("final_site", "document",cannonical_url)
						.setSource(jsonBuilder()
								.startObject()
								
								.field("HTTPheader",HTTPheader)
								.field("title",title) 
								.field("text",htmlText)
								.field("html_source",htmlDoc)
								.field("in_links",in_links)
								.field("out_links",out_links)
								.field("author",author)
								.field("depth",depth)
								.field("url",url)
								.endObject()
								)
						);
	
				BulkResponse bulkResponse = breq.execute().actionGet();

				//refresh indices 
				client.admin().indices().prepareRefresh("final_site").get();

				breq=client.prepareBulk(); 
				
				if(bulkResponse.hasFailures())
					System.out.println(bulkResponse.buildFailureMessage());
				else
					System.out.println("Document added to Index successfully :"+cannonical_url);
			}
			else
			{
				String index_author="";
				for ( SearchHit hit : response.getHits().hits()){  
					/*System.out.println("index header " +hit.getFields().get("getheader").getValue());
					System.out.println("index title  " +hit.getFields().get("gettitle").getValues());
					System.out.println("index text  " +hit.getFields().get("gettext").getValue());
					System.out.println("index htmlsource  " +hit.getFields().get("gethtmlsource").getValue());
					System.out.println("index inlinks  " +hit.getFields().get("getinlinks").getValue());
					System.out.println("index outlinks  " +hit.getFields().get("getoutlinks").getValue());
					System.out.println("author from index: ([Kaushik,Manognya])  " +hit.getFields().get("getauthors").getValues());
					System.out.println("index depth  " +hit.getFields().get("getdepth").getValue());
					System.out.println("index  url " +hit.getFields().get("geturl").getValue());
*/
					//index_author=
									
					/*merged_header=merged_header.concat(" "+(String) hit.getFields().get("getheader").getValue()+" ");
					merged_title=merged_title.concat(" "+hit.getFields().get("gettitle").getValues()+" ");
					merged_text=merged_text.concat(" "+(String) hit.getFields().get("gettext").getValue()+" ");
					merged_htmlsource=merged_htmlsource.concat(" "+(String) hit.getFields().get("gethtmlsource").getValue()+" ");
*/					merged_inlinks=merged_inlinks.concat(" "+(String) hit.getFields().get("getinlinks").getValue()+" ");
					merged_outlinks=merged_outlinks.concat(" "+(String) hit.getFields().get("getoutlinks").getValue()+" ");
					merged_author_list=hit.getFields().get("getauthors").values();
					
					merged_depth=merged_depth.concat(" "+hit.getFields().get("getdepth").getValue()+" ");
					//merged_url=merged_url.concat(" "+(String) hit.getFields().get("geturl").getValue()+" ");
				} 

				if(!merged_author_list.contains(author))
				{
/*					merged_header = merged_header.concat(HTTPheader);
					merged_title = merged_title.concat(title);
					merged_text = merged_text.concat(htmlText);
					merged_htmlsource = merged_htmlsource.concat(htmlDoc);
*/					merged_inlinks = merged_inlinks.concat(in_links);
					merged_outlinks = merged_outlinks.concat(out_links);
					merged_author_list.add(author);
					merged_depth = merged_depth.concat(depth);
//					//merged_url = merged_url.concat(url);   

					

					//System.out.println("merged author list locally: "+merged_author_list);
					breq.add(client.prepareIndex("final_site", "document",cannonical_url)
							.setSource(jsonBuilder()
									.startObject()
									.field("HTTPheader",HTTPheader)
									.field("title",title)
									.field("text",htmlText)
									.field("html_source",htmlDoc)
									.field("in_links",merged_inlinks)
									.field("out_links",merged_outlinks)
									.field("author",merged_author_list)
									.field("depth",merged_depth)
									.field("url",url)
									.endObject()
									)
							);

					BulkResponse bulkResponse = breq.execute().actionGet();

					//refresh indices
					client.admin().indices().prepareRefresh("final_site").get();
					breq=client.prepareBulk(); 
					if(bulkResponse.hasFailures())
						System.out.println(bulkResponse.buildFailureMessage());
					else
						System.out.println("Document Merged to the existing ID successfully:"+cannonical_url);
				}
			} 
		}
	}
}
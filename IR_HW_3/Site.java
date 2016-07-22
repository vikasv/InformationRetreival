import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Site {
	public String url;
	public String cannonicalUrl;
	public int level;
	public int inLinksCount;
	public int outLinkCount;
	public List<String> inLinkUrls= new ArrayList<>();
	public List<String> outLinkUrls= new ArrayList<>();
	public String domain;
	public int relevancy;
	public String author;
	public Map<String, List<String>> headers;
	public String title;
	
	Site()
	{
		this.inLinksCount=0;
		this.relevancy=0;
		this.level=0;
		this.inLinksCount=0;
		this.author="Vikas";
	}
	
}

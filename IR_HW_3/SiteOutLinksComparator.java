import java.util.Comparator;

public class SiteOutLinksComparator implements Comparator<Site>{

	public int compare(Site one, Site two)
	{
		if(one.outLinkCount<two.outLinkCount)
			return 1;
		else if (one.outLinkCount==two.outLinkCount) {
			return 0;
		}
		else
			return -1;
		
	}
}

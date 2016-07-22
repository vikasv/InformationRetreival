import java.util.Comparator;

public class SiteRelevancyComparator implements Comparator<Site>{

	public int compare(Site one, Site two)
	{
		if(one.relevancy<two.relevancy)
			return 1;
		else if (one.relevancy==two.relevancy) {
			return 0;
		}
		else
			return -1;
	}
}

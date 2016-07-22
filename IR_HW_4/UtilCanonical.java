import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;

public class UtilCanonical {
	
	public static String cannonical_url(String urlStr) throws MalformedURLException, UnsupportedEncodingException {
		String curDomain = "";
		String path = "";
		String final_return=urlStr;
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


	public static String canonicalDomain(URL url) {
		int port = url.getPort();
		if ((url.getProtocol().equals("http") && port == 80) || (url.getProtocol().equals("https") && port == 443)) {
			port = -1;
		}
		return (url.getHost() + (port == -1 ? "" : ":" + port)).toLowerCase();
	}

}

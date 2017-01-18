package edu.nd.sirs.websitesearch;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class CrawlStrategy extends WebCrawler {

	private static Logger logger = LoggerFactory
			.getLogger(CrawlStrategy.class);
	
	private final static Pattern FILTERS = Pattern
			.compile(".*(\\.(css|js|bmp|gif|jpe?g"
					+ "|png|tiff?|mid|mp2|mp3|mp4"
					+ "|wav|avi|mov|mpeg|ram|m4v|pdf"
					+ "|rm|smil|wmv|swf|wma|zip|rar|gz))$");

	boolean configured = false;

	String doctags = "WebCrawlerTags";
	String exacttags = "WebCrawlerExactTags";
	String fieldtags = "WebCrawlerFieldTags";

	String hostname;
	File crawlStore;
	Calendar c = Calendar.getInstance();

	/**
	 * Initializes the Crawler Strategy from the CustomData
	 */
	public void init() {
		CustomData data = (CustomData) this.getMyController().getCustomData();
		hostname = data.getHost();
		crawlStore = data.getCrawlStore();
		configured = true;
	}

	@Override
	public boolean shouldVisit(Page page, WebURL url) {
		if (!configured)
			init();
		String href = url.getURL().toLowerCase();

		logger.info("Considering " + href + "(" + hostname + ")");

		this.getMyController().getCustomData();
		return !FILTERS.matcher(href).matches() && href.contains(hostname);
	}

	@SuppressWarnings("deprecation")
	@Override
	public void visit(Page page) {
		if (!configured)
			init();
		String url = page.getWebURL().getURL();
		logger.trace("URL: " + url);

		if (page.getParseData() instanceof HtmlParseData) {
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
			String html = htmlParseData.getHtml();

			Map<String, String> docProperties = new HashMap<String, String>();
			docProperties.put("encoding", "UTF-8");
			docProperties.put("URL", url);
			c.setTimeInMillis(System.currentTimeMillis());
			docProperties.put("time", c.getTime().toGMTString());

			FileOutputStream outputStream = null;
			try {
				outputStream = new FileOutputStream(crawlStore + "/"
						+ URLEncoder.encode(url, "UTF-8"));
			} catch (FileNotFoundException e1) {
				logger.error("File Not found", e1);
			} catch (UnsupportedEncodingException e) {
				logger.error("UTF not supported", e);
			}

			try {
				outputStream.write(html.getBytes("UTF-8"));
			} catch (IOException e) {
				logger.error("Error writing html", e);
			}
		}
	}
}

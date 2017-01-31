package edu.nd.sirs.websitesearch;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class CrawlerProcess {

	private static Logger logger = LoggerFactory
			.getLogger(CrawlerProcess.class);

	private String host;
	private String tmpFolder;
	private String crawlFolder;

	/**
	 * Constructor for CrawlerProcess
	 * 
	 * @param tmpFolder
	 *            folder used by Crawler4j to store the frontier and other
	 *            information
	 * @param crawlFolder
	 *            folder used to store the downloaded data
	 */
	public CrawlerProcess(String tmpFolder, String crawlFolder) {
		this.tmpFolder = tmpFolder;
		this.crawlFolder = crawlFolder;
	}

	/**
	 * Configures and starts the Crawler4j Crawler.
	 * 
	 * @param linkdepth
	 *            Depth to go from url.
	 * @param url
	 *            Crawler starting point
	 */
	public void crawl(int linkdepth, String url) {
		host = url;
		String crawlStorageFolder = tmpFolder;
		int numberOfCrawlers = 1;

		CrawlConfig config = new CrawlConfig();
		config.setCrawlStorageFolder(crawlStorageFolder);
		config.setMaxDepthOfCrawling(linkdepth);

		/*
		 * Instantiate the controller for this crawl.
		 */
		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig,
				pageFetcher);
		CrawlController controller = null;
		try {
			controller = new CrawlController(config, pageFetcher,
					robotstxtServer);
		} catch (Exception e) {
			logger.debug("Error in Controller", e);			
		}

		/*
		 * For each crawl, you need to add some seed urls. These are the first
		 * URLs that are fetched and then the crawler starts following links
		 * which are found in these pages
		 */
		controller.addSeed(host);
		
		//we dont care about host prefix http://www.*
		host = host.replaceFirst("http://", "");
		host = host.replace("www.", "");

		/*
		 * Set the custom data for the crawler
		 */
		CustomData dataPacket = new CustomData(host, new File(crawlFolder));
		controller.setCustomData(dataPacket);

		/*
		 * Start the crawl. This is a blocking operation, meaning that your code
		 * will reach the line after this only when crawling is finished.
		 */
		controller.start(CrawlStrategy.class, numberOfCrawlers);

	}

	private static final String TMP = "./data/crawl/tmp";
	private static final String CRL = "./data/crawl";
	private static final String URL = "http://www.nd.edu/";
	private static final Integer DPT = 4;

	public static void main(String[] args) {

		File tmp = null;
		File crawl = null;
		String url = URL;
		int depth = DPT;

		if (args.length == 4) {
			logger.info("Using user provided parameters");
			try {
				tmp = new File(args[0]);
				crawl = new File(args[1]);
				url = args[2];
				depth = Integer.parseInt(args[3]);
			} catch (Exception e) {
				printUsage(e);
			}
		} else {
			logger.info("User did not provide 4 input arguments; reverting to defaults...");
			tmp = new File(TMP);
			crawl = new File(CRL);
			url = URL;
			depth = DPT;
		}

		try {
			tmp.mkdirs();
			crawl.mkdirs();
		} catch (Exception e) {
			logger.error("Error creating folders", e);
		}

		CrawlerProcess sc = new CrawlerProcess(tmp.getAbsolutePath(),
				crawl.getAbsolutePath());

		sc.crawl(depth, url);
	}

	private static void printUsage(Exception e) {
		logger.error(
				"Error parsing user provided parameters: "
						+ "CrawlerProcess <Crawler4jTmpFolder> <crawlerDataFolder> <SeedURL> <Depth>",
				e);
	}

}

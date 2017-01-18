package edu.nd.sirs.websitesearch;

import java.io.File;

public class CustomData {

	String host;
	File crawlStore;

	/**
	 * Data passed from the CrawlerProcess to the CrawlStrategy class so
	 * crawling decisions can be made.
	 * 
	 * @param host
	 *            hostname of the Web site we are crawling
	 * @param crawlStore
	 *            folder where the Web pages are being stored
	 */
	public CustomData(String host, File crawlStore) {
		super();
		this.host = host;
		this.crawlStore = crawlStore;
	}

	public String getHost() {
		return host;
	}

	public File getCrawlStore() {
		return crawlStore;
	}

}

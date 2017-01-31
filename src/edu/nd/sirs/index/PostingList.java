package edu.nd.sirs.index;

import java.util.ArrayList;
import java.util.List;

/**
 * Postings class with termId and <docID and frequency> postings.
 * 
 * @author tweninge
 *
 */
public class PostingList {

	private long term;
	private int df;
	private List<Posting> postings;
	

	/**
	 * Constructor from index reader
	 * 
	 * @param line
	 *            tab separated Posting
	 */
	public PostingList(String line) {
		postings = new ArrayList<Posting>();
		
		//3451:4	(180,1);(181,1);(387,1);(388,1);
		String[] s = line.split("\t");
		String[] dict = s[0].split(":");		
		term = Long.parseLong(dict[0]);
		df = Integer.parseInt(dict[1]);
		
		String[] pl = s[1].split(";");
		for(String p : pl){
			p = p.substring(1, p.length()-1); //strip parents
			String[] x = p.split(",");
			int doc = Integer.parseInt(x[0]);
			int frequency = Integer.parseInt(x[1]);
			Posting post = new Posting(doc, frequency);
			postings.add(post);
		}
		
	}

	public long getTermId() {
		return term;
	}

	public long getDocumentFrequency() {
		return df;
	}
	
	public List<Posting> getPostings() {
		return postings;
	}

	public int size() {
		return postings.size();
	}
}

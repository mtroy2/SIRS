package edu.nd.sirs.index;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import edu.nd.sirs.docs.Field;

/**
 * Postings class with termId and <docID and frequency> postings.
 * 
 * @author tweninge
 *
 */
public class PostingList {

	private long term;
	private int df;
	private TreeMap<Field, List<Posting>> postings;

	/**
	 * Constructor from index reader
	 * 
	 * @param line
	 *            tab separated Posting
	 */
	public PostingList(String line) {
		postings = new TreeMap<Field, List<Posting>>();

		// 3451:4
		// #0#1(180,1);(181,1);(387,1);(388,1);#2(180,1);(181,1);(387,1);(388,1);
		String[] s = line.split("\t");
		String[] dict = s[0].split(":");
		term = Long.parseLong(dict[0]);
		df = Integer.parseInt(dict[1]);

		String[] fields = s[1].split("#");
		for (String f : fields) {
			if (f.trim().isEmpty() ) continue;
			List<Posting> posts = new ArrayList<Posting>();
			int idx = f.indexOf('(');
			if (idx == -1) {
				postings.put(new Field(Integer.parseInt(f)), posts);
			} else {
				Field field = new Field(Integer.parseInt(f
						.substring(0, idx)));

				String[] pl = f.substring(idx).split(";");
				for (String p : pl) {
					p = p.substring(1, p.length() - 1); // strip parents
					String[] x = p.split(",");
					int doc = Integer.parseInt(x[0]);
					int frequency = Integer.parseInt(x[1]);
					Posting post = new Posting(doc, frequency);
					posts.add(post);
				}
				postings.put(field, posts);
			}
		}

	}

	public long getTermId() {
		return term;
	}

	public long getDocumentFrequency() {
		return df;
	}

	public List<Posting> getPostings(Field f) {
		return postings.get(f);
	}

	public int size(Field f) {
		return postings.get(f).size();
	}
}

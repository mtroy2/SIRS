package edu.nd.sirs.index;

/**
 * Postings class with termId, docID and frequency. Must implement Comparable
 * 
 * @author tweninge
 *
 */
public class Posting implements Comparable<Posting> {

	private long term;
	private int doc;
	private int frequency;

	/**
	 * Constructor from Indexer
	 * 
	 * @param termId
	 *            Token/Term Id
	 * @param docId
	 *            Document Id
	 * @param frequency
	 *            Number of times the term appears in the document
	 */
	public Posting(long termId, int docId, int frequency) {
		this.term = termId;
		this.doc = docId;
		this.frequency = frequency;
	}

	/**
	 * Constructor from index reader
	 * 
	 * @param line
	 *            tab separated Posting
	 */
	public Posting(String line) {
		String[] s = line.split("\t");
		term = Long.parseLong(s[0]);
		doc = Integer.parseInt(s[1]);
		frequency = Integer.parseInt(s[2]);
	}

	public long getTermId() {
		return term;
	}

	public int getDocId() {
		return doc;
	}

	public int getFrequency() {
		return frequency;
	}

	public void incrementFrequency() {
		frequency++;
	}

	/**
	 * Sorts by termId then documentId
	 */
	public int compareTo(Posting o) {
		if (term < o.term) {
			return -1;
		} else if (term == o.term) {
			if (doc < o.doc) {
				return -1;
			} else if (doc == o.doc) {
				return 0;
			} else {
				return 1;
			}
		} else {
			return 1;
		}
	}

}

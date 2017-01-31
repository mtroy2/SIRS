package edu.nd.sirs.index;

/**
 * Pair class with termId, docID and frequency. Must implement Comparable
 * 
 * @author tweninge
 *
 */
public class DocumentTerm implements Comparable<DocumentTerm> {

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
	public DocumentTerm(long termId, int docId, int frequency) {
		this.term = termId;
		this.doc = docId;
		this.frequency = frequency;
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
	public int compareTo(DocumentTerm o) {
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

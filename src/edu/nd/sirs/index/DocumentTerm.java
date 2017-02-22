package edu.nd.sirs.index;

import edu.nd.sirs.docs.Field;

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
	private Field f;

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
	public DocumentTerm(long termId, int docId, int frequency, Field f) {
		this.term = termId;
		this.doc = docId;
		this.frequency = frequency;
		this.f = f;
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

	public Field getField() {
		return f;
	}
	
	@Override
	public String toString(){
		return f.field + ";" + term + ";" + doc;
	}

	/**
	 * Sorts by termId then documentId then fieldid
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

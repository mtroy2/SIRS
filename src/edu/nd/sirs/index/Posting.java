package edu.nd.sirs.index;

/**
 * Posting with a docid and a frequency
 * 
 * @author tweninge
 *
 */
public class Posting {

	int docid;
	int frequency;
	
	public Posting(int doc, int frequency) {
		this.docid = doc;
		this.frequency = frequency;
	}

	public int getDocid() {
		return docid;
	}

	public int getFrequency() {
		return frequency;
	}

}

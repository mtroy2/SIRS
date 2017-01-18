package edu.nd.sirs.query;

/**
 * Basic hit accumulator
 * 
 * @author tweninge
 *
 */
public class Hit {

	private int docid;
	private float score;
	private short occurrence;

	/**
	 * Simple Constructor
	 * 
	 * @param docid
	 *            Document ID with a hit
	 */
	public Hit(int docid) {
		this.docid = docid;
	}

	public int getDocId() {
		return docid;
	}

	public float getScore() {
		return score;
	}

	public short getOccurrence() {
		return occurrence;
	}

	/**
	 * Adds score to the hit
	 * 
	 * @param update
	 */
	public void updateScore(double update) {
		this.score += update;
	}

	/**
	 * performs bitwise addition to update the number of occurrences
	 * 
	 * @param update
	 */
	public void updateOccurrence(short update) {
		this.occurrence |= update;
	}

}

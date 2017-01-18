package edu.nd.sirs.retrievalmodel;

import edu.nd.sirs.index.Posting;

/**
 * Retrieval model interface.
 * 
 * @author tweninge
 *
 */
public interface RetrievalModel {
	/**
	 * Returns a value for a given posting that is accumulated in the matcher
	 * 
	 * @param q
	 *            Posting to score
	 * @return score
	 */
	public float score(Posting q);
}

package edu.nd.sirs.retrievalmodel;

import edu.nd.sirs.index.Posting;

/**
 * Retrieval model interface.
 * 
 * @author tweninge
 *
 */
public interface IRetrievalModel {
	/**
	 * Returns a value for a given posting that is accumulated in the matcher
	 * 
	 * @param q
	 *            Posting to score
	 *            
	 * @param documentFrequency
	 *            number documents this term appears in
	 * @return score
	 */
	public float score(Posting q, long documentFrequency);
}

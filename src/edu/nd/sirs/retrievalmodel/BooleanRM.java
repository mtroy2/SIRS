package edu.nd.sirs.retrievalmodel;

import edu.nd.sirs.index.Posting;

/**
 * Boolean Retrieval Model
 * 
 * @author tweninge
 *
 */
public class BooleanRM implements RetrievalModel {

	/**
	 * Regardless of what the term is, or what document it is in, if it exists
	 * it gets a score of 1
	 */
	public float score(Posting q) {
		return 1.0f;
	}

}

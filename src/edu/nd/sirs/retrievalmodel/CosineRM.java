package edu.nd.sirs.retrievalmodel;

import edu.nd.sirs.index.DirectIndex;
import edu.nd.sirs.index.Posting;

/**
 * TF-IDF Retrieval Model
 * 
 * @author tweninge
 *
 */
public class CosineRM implements IRetrievalModel {

	/**
	 * Perform TF-IDF weighting for cosine similarity
	 */
	public float score(Posting q, long df) {
		return ((float)q.getFrequency()) * (float)(Math.log((float)DirectIndex.getInstance().getNumDocs()/(float)df)/Math.log(2));		
	}

}

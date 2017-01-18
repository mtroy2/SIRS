package edu.nd.sirs.index;

import edu.nd.sirs.index.Posting;

/**
 * Just like a regular posting, except with a run identifier for merging.
 * 
 * @author tweninge
 *
 */
public class MergePosting extends Posting {

	int run;

	/**
	 * Simple Constructor
	 * 
	 * @param p
	 *            Posting
	 * @param r
	 *            Run Id
	 */
	public MergePosting(Posting p, int r) {
		super(p.getTermId(), p.getDocId(), p.getFrequency());
		run = r;
	}

}

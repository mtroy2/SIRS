package edu.nd.sirs.index;

import edu.nd.sirs.index.DocumentTerm;

/**
 * Just like a regular posting, except with a run identifier for merging.
 * 
 * @author tweninge
 *
 */
public class MergeDocumentTerms extends DocumentTerm {

	int run;

	/**
	 * Simple Constructor
	 * 
	 * @param p
	 *            DocumentTerm
	 * @param r
	 *            Run Id
	 */
	public MergeDocumentTerms(DocumentTerm p, int r) {
		super(p.getTermId(), p.getDocId(), p.getFrequency());
		run = r;
	}

}

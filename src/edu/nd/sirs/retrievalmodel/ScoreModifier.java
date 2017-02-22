package edu.nd.sirs.retrievalmodel;

import edu.nd.sirs.docs.Field;
import edu.nd.sirs.index.InvertedIndex;
import edu.nd.sirs.query.ResultSet;
import edu.nd.sirs.query.Query;

/**
 * Score Modifier interface that is called after the accumulators and completed
 * 
 * @author tweninge
 *
 */
public interface ScoreModifier {
	/**
	 * Modifies the resultSet in some way
	 * 
	 * @param index
	 *            InvertedIndex
	 * @param queryTerms
	 *            Query
	 * @param resultSet
	 *            result set to modify
	 * @return true if modified, false if not
	 */
	boolean modifyScores(InvertedIndex index, Query queryTerms,
			ResultSet resultSet, Field f);
}

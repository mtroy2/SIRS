package edu.nd.sirs.retrievalmodel;

import edu.nd.sirs.docs.Field;
import edu.nd.sirs.docs.Fields;
import edu.nd.sirs.docs.HTMLDocument;
import edu.nd.sirs.index.DirectIndex;
import edu.nd.sirs.index.InvertedIndex;
import edu.nd.sirs.query.Query;
import edu.nd.sirs.query.ResultSet;

/**
 * Boolean Modifier that performs a basic intersection on all results.
 * 
 * @author tweninge
 *
 */
public class CosineScoreModifier implements ScoreModifier {

	/**
	 * Intersection
	 */
	public boolean modifyScores(InvertedIndex index, Query query,
			ResultSet resultSet, Field f) {
		float[] scores = resultSet.getScores();

		for (int i = 0; i < resultSet.getDocids().length; i++) {
			scores[i] = scores[i]
					/ DirectIndex
							.getInstance()
							.getDoc(resultSet.getDocids()[i],
									HTMLDocument.class).getNumTokens(f);
		}

		return true;
	}

}

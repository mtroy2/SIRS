package edu.nd.sirs.retrievalmodel;

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
	 * HW2 - Complete the Intersection
	 */
	public boolean modifyScores(InvertedIndex index, Query query,
			ResultSet resultSet) {
		
		//need to normalize based on document length
		
		return true;
	}

}

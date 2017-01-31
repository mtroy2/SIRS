package edu.nd.sirs.parser;

import java.util.List;

public interface INormalizer {

	/**
	 * Read and normalize a list of strings
	 * 
	 * @param str
	 *            String to normalize
	 * @return List of tokens
	 */
	List<String> normalize(List<String> str);

}

package edu.nd.sirs.parser;

import java.util.List;

public interface ITokenizer {

	/**
	 * Read and tokenize a String
	 * 
	 * @param str
	 *            String to tokenize
	 * @return List of tokens
	 */
	List<String> tokenize(String str);

}

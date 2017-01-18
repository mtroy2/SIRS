package edu.nd.sirs.query;

import java.util.ArrayList;
import java.util.List;

import edu.nd.sirs.parser.ITokenizer;
import edu.nd.sirs.parser.WhitespaceTextTokenizer;

/**
 * Query class keeps a list of terms as a query.
 * 
 * @author tweninge
 *
 */
public class Query {

	private String queryString;
	private ITokenizer tokenizer;
	private List<String> terms;

	/**
	 * Simple Constructor
	 * 
	 * @param queryString
	 */
	public Query(String queryString) {
		this(new WhitespaceTextTokenizer(), queryString);
	}

	/**
	 * Tokenizer Constructor
	 * 
	 * @param tok
	 *            Tokenizer object
	 * @param queryString
	 */
	public Query(ITokenizer tok, String queryString) {
		this.tokenizer = tok;
		this.queryString = queryString;
		this.terms = new ArrayList<String>();
		for (String s : parse()) {
			terms.add(s);
		}
	}

	public List<String> getTerms() {
		return terms;
	}

	/**
	 * Parse the Query
	 * 
	 * @return list of tokens
	 */
	private Iterable<String> parse() {
		return tokenizer.tokenize(queryString);
	}

}

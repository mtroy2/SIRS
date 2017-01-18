package edu.nd.sirs.docs;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.nd.sirs.parser.ITokenizer;
import edu.nd.sirs.parser.WhitespaceTextTokenizer;

/**
 * Document with only text to parse
 * 
 * @author tweninge
 *
 */
public class TextDocument extends Document {
	
	private static Logger logger = LoggerFactory.getLogger(TextDocument.class);

	/**
	 * Constructor from indexer
	 * 
	 * @param docId
	 *            document ID
	 * @param file
	 *            File to parse
	 */
	public TextDocument(Integer docId, File file) {
		super(docId, file);
	}

	/**
	 * Constructor from index reader
	 * 
	 * @param docId
	 *            document ID
	 * @param line
	 *            Text tokens to read
	 */
	public TextDocument(Integer docId, String line) {
		super(docId, line);
	}

	@Override
	public List<String> parse(Integer docId, File f) {
		ITokenizer tokenizer = new WhitespaceTextTokenizer();
		List<String> toks = new ArrayList<String>();

		try {
			FileReader fr = new FileReader(f);
			toks = tokenizer.tokenize(fr);
			fr.close();
		} catch (IOException e) {
			logger.error("Error in parser", e);
		}

		numTokens = toks.size();

		return toks;
	}

}

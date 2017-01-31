package edu.nd.sirs.docs;

import java.io.InputStream;
import java.util.List;
import java.util.zip.ZipEntry;

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
	public TextDocument(Integer docId, ZipEntry file) {
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
	public List<String> parse(Integer docId, InputStream fileInputStream) {
		ITokenizer tokenizer = new WhitespaceTextTokenizer();
		List<String> toks = tokenizer.tokenize(this.readFile(fileInputStream));

		numTokens = toks.size();

		return toks;
	}

}

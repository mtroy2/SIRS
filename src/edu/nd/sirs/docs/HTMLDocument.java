package edu.nd.sirs.docs;

import java.io.InputStream;
import java.util.List;
import java.util.zip.ZipEntry;

import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.nd.sirs.parser.CaseFoldingNormalizer;
import edu.nd.sirs.parser.WhitespaceTextTokenizer;

/**
 * Document with only text to parse
 * 
 * @author tweninge
 *
 */
public class HTMLDocument extends Document {
	
	private static Logger logger = LoggerFactory.getLogger(HTMLDocument.class);

	/**
	 * Constructor from indexer
	 * 
	 * @param docId
	 *            document ID
	 * @param file
	 *            File to parse
	 */
	public HTMLDocument(Integer docId, ZipEntry file) {
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
	public HTMLDocument(Integer docId, String line) {
		super(docId, line);
	}

	@Override
	public List<String> parse(Integer docId, InputStream fileInputStream) {
		logger.info("HTML Parsing invoked");
		String content = this.readFile(fileInputStream);
		
		org.jsoup.nodes.Document doc = Jsoup.parse(content);
		String text = doc.text();
		WhitespaceTextTokenizer tokenizer = new WhitespaceTextTokenizer();
		List<String> tokens = tokenizer.tokenize(text);
		CaseFoldingNormalizer normalizer = new CaseFoldingNormalizer();
		List<String> normTokens = normalizer.normalize(tokens);
		
		numTokens = normTokens.size();
		
		return normTokens;
	}

}

package edu.nd.sirs.docs;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
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
	public List<Token> parse(Integer docId, InputStream fileInputStream) {
		logger.info("HTML Parsing invoked");
		Fields.getInstance().addField("body");
		Fields.getInstance().addField("link");
		Fields.getInstance().addField("title");

		String html = this.readFile(fileInputStream);
		List<Token> tokens = new ArrayList<Token>();

		WhitespaceTextTokenizer tokenizer = new WhitespaceTextTokenizer();
		CaseFoldingNormalizer normalizer = new CaseFoldingNormalizer();

		org.jsoup.nodes.Document doc = Jsoup.parse(html);
		Elements h = doc.getElementsByTag("head");
		int numtitletokens = 0;
		if (h.size() >= 1) {
			Elements x = h.get(0).getElementsByTag("title");
			if (x.size() >= 1) {
				this.resources.put("title", x.get(0).text().replaceAll("\n", " "));
				List<String> title = tokenizer.tokenize(x.get(0).text());
				title = normalizer.normalize(title);
				for (String s : title) {
					tokens.add(new Token(s, Fields.getInstance().getFieldId(
							"title")));
					numtitletokens++;
				}
			}
		}
		numTokens.put(Fields.getInstance().getFieldId("title"), numtitletokens);

		String text = doc.getElementsByTag("body").text();

		List<String> content = tokenizer.tokenize(text);
		content = normalizer.normalize(content);
		int contenttoks = 0;
		for (String s : content) {
			tokens.add(new Token(s, Fields.getInstance().getFieldId("body")));
			contenttoks++;
		}
		numTokens.put(Fields.getInstance().getFieldId("body"), contenttoks);

		Elements as = doc.getElementsByTag("a");
		for (Element a : as) {
			String url = a.absUrl("href");
			List<Token> anchorToks = new ArrayList<Token>();
			List<String> anchor = tokenizer.tokenize(a.text());
			anchor = normalizer.normalize(anchor);
			for (String s : anchor) {
				anchorToks.add(new Token(s, Fields.getInstance().getFieldId(
						"link")));
			}
			this.resources.put("l" + url, anchorToks);
		}

		return tokens;
	}


}

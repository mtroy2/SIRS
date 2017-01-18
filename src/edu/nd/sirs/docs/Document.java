package edu.nd.sirs.docs;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract class that represents a general Document.
 * 
 * @author tweninge
 *
 */
public abstract class Document {
	protected String name;
	protected int docId;
	protected int numTokens;
	protected Map<String, String> resources;

	/**
	 * Constructor from indexer
	 * 
	 * @param docId
	 *            document ID
	 * @param file
	 *            File to parse
	 */
	public Document(Integer docId, File file) {
		this.docId = docId;
		this.name = file.getName();
		this.numTokens = 0;
		resources = new HashMap<String, String>();
	}

	/**
	 * Constructor from index reader
	 * 
	 * @param docId
	 *            document ID
	 * @param line
	 *            Text tokens to read
	 */
	public Document(Integer docId, String line) {
		this.docId = docId;
		this.name = "";
		this.numTokens = 0;
		resources = new HashMap<String, String>();
		readFromIndex(line);
	}

	public String getName() {
		return name;
	}

	public int getDocId() {
		return docId;
	}

	public int getNumTokens() {
		return numTokens;
	}

	/**
	 * Creates a String to write to direct document index including all extra
	 * information
	 * 
	 * Expected to be overridden by subclasses
	 * 
	 * @return String representation of a document
	 */
	public String writeToIndex() {
		StringBuffer sb = new StringBuffer();

		sb.append(getDocId() + "\t" + getName() + "\t" + getNumTokens());
		for (Map.Entry<String, String> e : resources.entrySet()) {
			sb.append("\t" + e.getKey() + ":" + e.getValue());
		}
		sb.append("\n");

		return sb.toString();
	}

	/**
	 * Reads data that was previous written to file by writeToIndex() function.
	 * 
	 * Should be overridden when writeToIndex is overridden
	 * 
	 * @param line
	 *            line to read
	 */
	public void readFromIndex(String line) {
		String[] s = line.split("\t");
		docId = Integer.parseInt(s[0]);
		name = s[1];
		numTokens = Integer.parseInt(s[2]);
		for (int i = 3; i < s.length; i++) {
			String[] r = s[i].split(":");
			resources.put(r[0], r[1]);
		}
	}

	/**
	 * Parse the file. This function uses should parse a file and return
	 * normalized tokens for indexing.
	 * 
	 * @param docId
	 *            document ID
	 * @param file
	 *            File to parse
	 * @return Collection of Text Tokens
	 */
	public abstract List<String> parse(Integer docId, File file);

}

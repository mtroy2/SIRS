package edu.nd.sirs.docs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;

/**
 * Abstract class that represents a general Document.
 * 
 * @author tweninge
 *
 */
public abstract class Document {
	protected String name;
	protected int docId;
	protected Map<Field, Integer> numTokens;
	protected Map<String, Object> resources;

	/**
	 * Constructor from indexer
	 * 
	 * @param docId
	 *            document ID
	 * @param file
	 *            File to parse
	 */
	public Document(Integer docId, ZipEntry file) {
		this.docId = docId;
		this.name = file.getName().replace("crawl/", "");
		this.numTokens = new HashMap<Field, Integer>();
		resources = new HashMap<String, Object>();
	}

	/**
	 * Constructor from index reader
	 * 
	 * @param docId
	 *            document ID
	 * @param line
	 *            Text tokens to read
	 */
	protected Document(Integer docId, String line) {
		this.docId = docId;
		this.name = "";
		this.numTokens = new HashMap<Field, Integer>();
		resources = new HashMap<String, Object>();
		readFromIndex(line);
	}

	public String getName() {
		return name;
	}

	public int getDocId() {
		return docId;
	}

	public int getNumTokens(Field f) {
		return numTokens.get(f);
	}

	public Map<String, Object> getResources(){
		return resources;
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

		sb.append(getDocId() + "\t" + getName() + "\t" + printNumTokens());
		for (Map.Entry<String, Object> e : resources.entrySet()) {
			sb.append("\t" + e.getKey() + "-#-" + e.getValue());
		}
		sb.append("\n");

		return sb.toString();
	}

	private String printNumTokens() {
		StringBuffer sb = new StringBuffer();
		for (Entry<Field, Integer> e : numTokens.entrySet()) {
			sb.append(e.getKey().field + ":" + e.getValue() + ",");
		}
		return sb.toString().substring(0, sb.length() - 1);
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
		String[] numtoksStr = s[2].split(",");
		for (int i = 0; i < numtoksStr.length; i++) {
			String[] r = numtoksStr[i].split(":");
			Field f = new Field(Integer.parseInt(r[0]));
			numTokens.put(f, Integer.parseInt(r[1]));
		}

		for (int i = 3; i < s.length; i++) {
			String[] r = s[i].split("-#-");
			if (r.length == 2) {				
				r[1].replaceAll("\"", "");
				resources.put(r[0], r[1]);
			}
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
	public abstract List<Token> parse(Integer docId, InputStream fileInputStream);

	protected String readFile(InputStream fileInputStream) {
		StringBuffer contentBuffer = new StringBuffer();
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fileInputStream, "UTF-8"));
			String line = "";

			while ((line = br.readLine()) != null) {
				contentBuffer.append(line).append("\n");
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return contentBuffer.toString();
	}

}

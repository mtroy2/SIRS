package edu.nd.sirs.index;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lexicon singleton class handles reading and writing to the lexicon on disk
 * 
 * @author tweninge
 *
 */
public class Lexicon {
	private static Logger logger = LoggerFactory.getLogger(Lexicon.class);

	private static final String LEXICON = "./data/lex.txt";

	private static Lexicon me = null;
	private RandomAccessFile lex;
	private long length;

	/**
	 * Singleton constructor, use getInstance()
	 */
	private Lexicon() {
		try {
			lex = new RandomAccessFile(LEXICON, "r");
			length = lex.length();
		} catch (IOException e) {
			logger.error("Cannot find lexicon file", e);
		}
	}

	/**
	 * Singleton instance getter.
	 * 
	 * @return InvertedIndex object
	 */
	public static Lexicon getInstance() {
		if (me == null) {
			me = new Lexicon();
		}

		return me;
	}

	/**
	 * Perform binary search to find term id from lexicon file.
	 * 
	 * @param term
	 *            String term token
	 * @return termId corresponding to term or -1 if term not found
	 */
	public int getTermId(String term) {
		long low = 0;
		long high = length;
		long cur = high;
		int x = -3;
		do {
			if (high - low < 200) {
				return scanToFind(low, high, term);
			}
			if (x == -3) {
				cur = low + (high - low) / 2;
			} else if (x == -1) {
				cur = low + (high - low) / 2;
			}
			x = compareTo(cur, term);
			if (x >= 0) {
				return x;
			} else if (x == -3) {
				low = cur;
			} else { // -1
				high = cur;
			}
		} while (x < 0);

		return -1;
	}

	/**
	 * Scan to find the termId, typically we do this after we get close using
	 * the binary search method.
	 * 
	 * @param pos
	 *            position to start searching at
	 * @param high
	 *            position to search until
	 * @param term
	 *            term we're looking for
	 * @return termId corresponding to term or -1 if term not found
	 */
	private int scanToFind(long pos, long high, String term) {
		try {
			lex.seek(pos);
			if (pos < 200) {
				pos = 0;
			} else {
				lex.readLine();
			}
			String l = "";
			while ((l = lex.readLine()) != null) {
				String[] line = l.split("\t");
				if (line[0].equals(term)) {
					return Integer.parseInt(line[1]);
				}

				if (lex.getFilePointer() > high) {
					return -1;
				}
			}
		} catch (IOException e) {
			logger.error("Error reading file", e);
		}
		return -1;
	}

	/**
	 * Reads the lexicon index at this position
	 * 
	 * @param pos
	 *            file position
	 * @param term
	 *            Term to look for
	 * @return term termId if term is found otherwise -3 if location lower or -1
	 *         if location higher
	 */
	private int compareTo(long pos, String term) {
		try {
			lex.seek(pos);
			lex.readLine(); // get to the end of the line
			String[] line = lex.readLine().split("\t");
			if (line[0].equals(term)) {
				return Integer.parseInt(line[1]);
			} else {
				return line[0].compareTo(term) < 0 ? -3 : -1;
			}

		} catch (IOException e) {
			logger.error("Error reading file", e);
		}

		return -3;
	}

	/**
	 * Simple testing main method
	 * 
	 * @param args
	 *            none needed
	 */
	public static void main(String[] args) {
		Lexicon lex = Lexicon.getInstance();
		System.out.println(lex.getTermId("Web"));
	}
}

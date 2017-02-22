package edu.nd.sirs.index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.nd.sirs.docs.Fields;

/**
 * Inverted Index singleton class handles reading and writing to the inverted
 * index on disk
 * 
 * @author tweninge
 *
 */
public class InvertedIndex {
	private static Logger logger = LoggerFactory.getLogger(InvertedIndex.class);

	private static final String IDX = "./data/idx.txt";
	private static final String IDXTERMOFFSET = "./data/idx_term_offset.txt";

	private static InvertedIndex me = null;
	private long[] offsets;
	private RandomAccessFile idx;

	/**
	 * Singleton constructor, use getInstance()
	 */
	private InvertedIndex() {
		try {
			logger.info("Creating InvertedIndex singleton object.");
			idx = new RandomAccessFile(IDX, "r");
			loadOffsets();
			
			loadFields();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void loadFields() throws IOException {
		idx.seek(0);
		String fieldString = idx.readLine();
		Fields.loadFromInvertedIndex(fieldString);
	}

	/**
	 * Load the offsets into memory
	 * 
	 * @throws IOException
	 */
	private void loadOffsets() throws IOException {
		logger.info("loading term offsets into memory.");
		BufferedReader br = new BufferedReader(new FileReader(IDXTERMOFFSET));
		String line = br.readLine(); // number of terms
		int terms = Integer.parseInt(line);
		offsets = new long[terms];

		for (int term = 0; (line = br.readLine()) != null; term++) {
			offsets[term] = Long.parseLong(line);
		}
		br.close();
	}

	/**
	 * Singleton instance getter.
	 * 
	 * @return InvertedIndex object
	 */
	public static InvertedIndex getInstance() {
		if (me == null) {
			me = new InvertedIndex();
		}

		return me;
	}

	/**
	 * Uses the offset data to do disk seek and retrieve posting from disk
	 * 
	 * @param termid
	 * @return
	 */
	public PostingList getPostings(int termid) {		
		long offset = offsets[termid]; 
		try {
			idx.seek(offset);
			long x = System.currentTimeMillis();
			String line = idx.readLine();
			if (line == null || line.isEmpty())
				logger.error("Something wrong reading posting");
			PostingList p = new PostingList(line);
			System.out.println(System.currentTimeMillis() - x);
			if (termid != p.getTermId())
				logger.error("Cannot read termid in postings list");
			return p;

		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Simple testing main method
	 * 
	 * @param args
	 *            none needed
	 */
	public static void main(String[] args) {
		InvertedIndex idx = InvertedIndex.getInstance();
		PostingList x = idx.getPostings(100);
		System.out.println(x);
	}

}

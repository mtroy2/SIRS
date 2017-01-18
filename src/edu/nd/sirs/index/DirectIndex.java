package edu.nd.sirs.index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.nd.sirs.docs.Document;
import edu.nd.sirs.docs.TextDocument;

/**
 * Direct Index singleton class handles reading and writing to the direct
 * document index on disk
 * 
 * @author tweninge
 *
 */
public class DirectIndex {
	private static Logger logger = LoggerFactory.getLogger(DirectIndex.class);

	private static final String DOCIDX = "./data/doc_idx.txt";
	private static final String DOCIDXOFFSET = "./data/doc_idx_offset.txt";

	private static DirectIndex me = null;
	private List<Long> offsets;
	private RandomAccessFile idx;

	/**
	 * Singleton constructor, use getInstance()
	 */
	private DirectIndex() {
		try {
			idx = new RandomAccessFile(DOCIDX, "r");
			loadOffsets();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Load the offsets into memory
	 * 
	 * @throws IOException
	 */
	private void loadOffsets() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(DOCIDXOFFSET));
		String line = br.readLine(); // number of terms
		offsets = new ArrayList<Long>();

		offsets.add(0l);
		while ((line = br.readLine()) != null) {
			offsets.add(Long.parseLong(line));
		}
		br.close();
	}

	/**
	 * Singleton instance getter.
	 * 
	 * @return InvertedIndex object
	 */
	public static DirectIndex getInstance() {
		if (me == null) {
			me = new DirectIndex();
		}

		return me;
	}

	/**
	 * Retrieves the document from the direct index. Loads appropriate Document
	 * class to read information.
	 * 
	 * @param docid
	 *            document Id
	 * @param d
	 *            Class of document to read, must extend Document
	 * @return Document object
	 */
	public Document getDoc(int docid, Class<? extends Document> d) {
		try {
			long offset = offsets.get(docid);
			idx.seek(offset);
			String line = idx.readLine();
			Constructor<? extends Document> c = d
					.getDeclaredConstructor(new Class[] { Integer.class,
							String.class });
			return d.cast(c.newInstance(new Object[] { docid, line }));
		} catch (InstantiationException e) {
			logger.error("Cannot instantiate class", e);
		} catch (IllegalAccessException e) {
			logger.error("Cannot access class", e);
		} catch (IllegalArgumentException e) {
			logger.error("Wrong argument passed to class", e);
		} catch (InvocationTargetException e) {
			logger.error("Class not found", e);
		} catch (NoSuchMethodException e) {
			logger.error("Constructor not found", e);
		} catch (SecurityException e) {
			logger.error("Cannot access class", e);
		} catch (IOException e) {
			logger.error("Cannot read from file", e);
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
		DirectIndex idx = DirectIndex.getInstance();
		idx.getDoc(85, TextDocument.class);
	}

}

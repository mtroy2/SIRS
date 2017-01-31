package edu.nd.sirs.index;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.nd.sirs.docs.Document;
import edu.nd.sirs.docs.TextDocument;

/**
 * Creates direct and inverted indexes for the documents stored in the folder.
 * 
 * @author tweninge
 *
 */
public class Indexer {

	private static Logger logger = LoggerFactory.getLogger(Indexer.class);

	private static final String DOCIDX = "./data/doc_idx.txt";
	private static final String DOCIDXOFFSET = "./data/doc_idx_offset.txt";
	private static final String LEXICON = "./data/lex.txt";
	private static final String RUNSPREFIX = "./data/runs/run";
	private static final String IDX = "./data/idx.txt";
	private static final String IDX_GAPS = "./data/idx_gaps.txt";
	private static final String IDX_VBE = "./data/idx_vbe.txt";
	private static final String IDX_GAMMA = "./data/idx_gamma.txt";
	private static final String IDXTERMOFFSET = "./data/idx_term_offset.txt";

	private static final Integer RUN_SIZE = 1000000;
	private static final Boolean COMPRESS = true;

	private int wordId;
	private int docId;
	private List<DocumentTerm> run;
	private int runNumber;

	private TreeMap<String, Integer> voc;

	/**
	 * Indexer Constructor
	 */
	public Indexer() {
		wordId = 0;
		docId = 0;
		runNumber = 0;
		voc = new TreeMap<String, Integer>();
	}

	/**
	 * Create direct and inverted indices for each file in the Zip file.
	 * 
	 * @param crawlFile 
	 */
	private void indexZip(File crawlFile) {
		docId = 0;

		PrintWriter docWriter;
		PrintWriter docWriterOffset;
		try {
			docWriter = new PrintWriter(DOCIDX);
			docWriterOffset = new PrintWriter(DOCIDXOFFSET);

			// start the first run
			logger.info("Starting the first indexer run.");
			run = new ArrayList<DocumentTerm>();
			int written = 0;
						
			ZipFile zip = new ZipFile(crawlFile);
			Enumeration<? extends ZipEntry> enties = zip.entries();

			while (enties.hasMoreElements()) {
				ZipEntry file = enties.nextElement();
				logger.info("Indexing document " + file.getName());
				Document doc = new TextDocument(docId, file);
				List<String> tokens = doc.parse(docId, zip.getInputStream(file));
				index(tokens);
				docWriterOffset.write(written + "\n");

				// Writing to Direct Index
				String idxable = doc.writeToIndex();
				docWriter.write(idxable);
				written += idxable.length();
				docId++;
			}
			zip.close();
			docWriter.close();
			docWriterOffset.close();

			// If there is something yet in the last run, sort it and store
			if (run.size() > 0) {
				logger.info("Writing file run to disk.");
				storeRun();
			}

			logger.info("Indexing runs complete.");
		} catch (IOException e) {
			logger.error("Cannot find direct index file.", e);
		}

		try {
			mergeRuns();
		} catch (FileNotFoundException e) {
			logger.error("Cannot find inverted index file.", e);
		}

		// Output the vocabulary
		try {
			outputLexicon();
		} catch (FileNotFoundException e) {
			logger.error("Cannot find lexicon file.", e);
		}
		logger.info("Indexing complete.");
	}

	private void outputLexicon() throws FileNotFoundException {
		logger.info("Writing lexicon to disk");
		PrintWriter lexFile = new PrintWriter(LEXICON);
		for (Entry<String, Integer> x : voc.entrySet()) {
			lexFile.println(x.getKey() + "\t" + x.getValue());
		}
		lexFile.close();
		logger.info("Lexicon writing finished");
	}

	/**
	 * Creates variable byte encoding see
	 * http://nlp.stanford.edu/IR-book/html/htmledition/variable-byte-codes-1.html
	 * 
	 * @param gapsArray
	 *            Array of gaps of the form [[gap, cnt], [gap, cnt]...]
	 * @return list of string-pairs of the form [[(char)vbe, cnt], [(char)vbe,
	 *         cnt]...] where cnt doesn't change, but the gap is encoded as a
	 *         VBE and resulting binary is cast to a char. For example: gap = 5
	 *         is VBE encoded as 10000101, which is 133 in decimal. (char)133 is
	 *         a` (a with a grave accent), which we write to the postings list
	 *         in unicode
	 */
	private List<String[]> variableByteEncoding(List<Integer[]> gapsArray) {
		List<String[]> vbeArr = new ArrayList<String[]>();

		for (Integer[] post : gapsArray) {
			String[] vbe = new String[2];

			int gap = post[0];
			String bin = Integer.toBinaryString(gap);
			List<String> size7pieces = new ArrayList<String>();
			StringBuilder size7 = new StringBuilder();
			for (int i = 0; i < bin.length(); i++) {
				if (size7.length() < 7) {
					size7.insert(0, bin.charAt(i));
				} else {
					size7pieces.add(size7.toString());
					size7 = new StringBuilder();
					size7.insert(0, bin.charAt(i));
				}
			}
			size7pieces.add(size7.toString());
			String output = "";
			for (int i = size7pieces.size() - 1; i > 0; i--) {
				String newbin = size7pieces.get(i);
				String padbin = "0" + StringUtils.leftPad(newbin, 7, '0');
				// System.out.print(Integer.parseInt(padbin, 2) + ": ");
				output += padbin;
			}

			String padbin = "1" + StringUtils.leftPad(bin, 7, '0');
			// System.out.print(Integer.parseInt(padbin, 2) + ": ");
			output += padbin;

			vbe[0] = Character.toString((char) Integer.parseInt(output, 2));
			vbe[1] = Integer.toString(post[1]);

			// System.out.println(output);

			vbeArr.add(vbe);
		}
		return vbeArr;
	}

	/**
	 * HW2 - you'll need to complete this function
	 * 
	 * Creates gamma encoding
	 * 
	 * @param gapsArray
	 *            Array of gaps of the form [[gap, cnt], [gap, cnt]...]
	 * @return list of string-pairs of the form [[(char)gamma-code, cnt],
	 *         [(char)gamma-code, cnt]...] where cnt doesn't change, but the gap
	 *         is gamma-encoded and resulting binary is cast to a char. For
	 *         example: gap = 5 is gamma-encoded as 11001, which is 25 in
	 *         decimal. (char)25 is EM (end of medium ASCII code), which we
	 *         write to the postings list in unicode.
	 */
	private List<String[]> gammaEncoding(List<Integer[]> gaps) {
		List<String[]> gammaArray = new ArrayList<String[]>();

		return gammaArray;
	}

	/**
	 * Creates a gap encoding list from a postings file:
	 * 
	 * @param postings
	 *            string of postings that look like this: (docid,
	 *            countInDoc);(docid, countInDoc)... for example (0,1);(1,2);...
	 *            means that the term (not shown in postings) is found in doc #0
	 *            1 time, and doc #1 2 times
	 * @return List of gaps
	 */
	private List<Integer[]> createGapEncoding(String postings) {
		List<Integer[]> postingsArray = postingsToList(postings);
		List<Integer[]> gapsArray = new ArrayList<Integer[]>();
		int gap = 0;
		for (Integer[] post : postingsArray) {
			Integer[] gapArray = new Integer[2];
			int docid = post[0];
			int cnt = post[1];
			gap = docid - gap;
			gapArray[0] = gap;
			gapArray[1] = cnt;
			gapsArray.add(gapArray);
		}
		return gapsArray;
	}

	/**
	 * Used in createGapEncoded to convert each posting to a pair of ints
	 * 
	 * @param postings
	 *            string of postings that look like this: (docid,
	 *            countInDoc);(docid, countInDoc)... for example (0,1);(1,2);...
	 *            means that the term (not shown in postings) is found in doc #0
	 *            1 time, and doc #1 2 times
	 * @return List of [[docid, cnt],[docid, cnt]] pairs
	 */
	private List<Integer[]> postingsToList(String postings) {
		List<Integer[]> postingsArray = new ArrayList<Integer[]>();
		for (String posting : postings.split(";")) {
			Integer[] postArray = new Integer[2];
			String[] post = posting.substring(1, posting.length() - 1).split(",");
			postArray[0] = Integer.parseInt(post[0]);
			postArray[1] = Integer.parseInt(post[1]);
			postingsArray.add(postArray);
		}
		return postingsArray;
	}

	private String encodingListToString(List<String[]> vbe) {
		String postings = "";
		for (String[] pair : vbe) {
			postings = postings + "(" + pair[0] + "," + pair[1] + ");";
		}
		return postings;
	}

	private String gapsListToString(List<Integer[]> vbe) {
		String postings = "";
		for (Integer[] pair : vbe) {
			postings = postings + "(" + pair[0] + "," + pair[1] + ");";
		}
		return postings;
	}

	/**
	 * Merge the runs together to make a single inverted index
	 * 
	 * @throws FileNotFoundException
	 */
	private void mergeRuns() throws FileNotFoundException {

		// Create the heap
		PriorityQueue<MergeDocumentTerms> mergeHeap = new PriorityQueue<MergeDocumentTerms>();
		List<RunFile> rfv = new ArrayList<RunFile>();
		String filename;
		DocumentTerm ocurr;
		MergeDocumentTerms ro;
		for (int i = 0; i < runNumber; ++i) {
			filename = RUNSPREFIX + i;
			rfv.add(new RunFile(new File(filename), RUN_SIZE / runNumber));
			// get the first element and put it in the heap
			ocurr = rfv.get(i).getRecord();
			if (ocurr == null) {
				logger.error("Error: Record was not found.");
				return;
			}
			ro = new MergeDocumentTerms(ocurr, i);
			mergeHeap.add(ro);
		}
		long currentTerm = 0l;
		long currentTermOffset = 0l;
		PrintWriter outFile = new PrintWriter(IDX);
		PrintWriter gaps_outFile = new PrintWriter(IDX_GAPS);
		PrintWriter vbe_outFile = new PrintWriter(IDX_VBE);
		PrintWriter gamma_outFile = new PrintWriter(IDX_GAMMA);
		PrintWriter tosFile = new PrintWriter(IDXTERMOFFSET);
		String wid = wordId + "\n";
		tosFile.print(wid);

		MergeDocumentTerms first;
		logger.info("Merging run files...");

		int df = 0;
		StringBuffer posting = new StringBuffer();

		while (!mergeHeap.isEmpty()) {
			first = mergeHeap.poll();

			// Get a new posting from the same run and
			// put it in the heap, if possible
			ocurr = rfv.get(first.run).getRecord();
			if (ocurr != null) {
				ro = new MergeDocumentTerms(ocurr, first.run);
				mergeHeap.add(ro);
			}
			// Saving to the file
			if (first.getTermId() > currentTerm) {
				tosFile.println(currentTermOffset);
				List<Integer[]> gaps = createGapEncoding(posting.toString());

				if (COMPRESS) {

					String gapsPosting = gapsListToString(gaps);

					List<String[]> vbe = variableByteEncoding(gaps);
					String vbePosting = encodingListToString(vbe);

					List<String[]> gamma = gammaEncoding(gaps);
					String gammaPosting = encodingListToString(gamma);

					String gaps_p = currentTerm + ":" + df + "\t" + gapsPosting + "\n";
					String vbe_p = currentTerm + ":" + df + "\t" + vbePosting + "\n";
					String gamma_p = currentTerm + ":" + df + "\t" + gammaPosting + "\n";

					gaps_outFile.print(gaps_p);
					vbe_outFile.print(vbe_p);
					gamma_outFile.print(gamma_p);
				}
				
				String p = currentTerm + ":" + df + "\t" + posting + "\n";
				outFile.print(p);
				currentTermOffset += p.getBytes().length;
				currentTerm = first.getTermId();
				posting = new StringBuffer();
				df = 0;
			} else if (first.getTermId() < currentTerm) {
				logger.error("Term ids messed up, something went wrong with the sorting");
			}

			df++;
			posting.append("(" + first.getDocId() + "," + first.getFrequency() + ");");

		}
		outFile.close();
		gaps_outFile.close();
		vbe_outFile.close();
		gamma_outFile.close();
		tosFile.close();
		logger.info("Index merging finished");
	}

	/**
	 * Creates a local vocabulary and indexes terms one-by-one
	 * 
	 * @param tokens
	 *            list of tokens for indexing
	 */
	private void index(List<String> tokens) {
		HashMap<Integer, DocumentTerm> lVoc = new HashMap<Integer, DocumentTerm>();
		for (String token : tokens) {
			if (token.equals("weninger")) {
				System.out.println();
			}
			index(token, docId, lVoc);
		}

		for (DocumentTerm p : lVoc.values()) {
			if (run.size() < RUN_SIZE) {
				run.add(p);
			} else {
				logger.info("Current indexing run full, storing to disk.");
				storeRun();
				run.add(p);
			}
		}
	}

	/**
	 * Store the current run on disk.
	 */
	private void storeRun() {
		// creating the output file
		try {
			long runId = getRunNumber();
			File outName = new File(RUNSPREFIX + runId);
			if (!outName.getParentFile().exists()) {
				logger.info("Creating run directory");
				outName.getParentFile().mkdir();
			}
			if (outName.exists()) {
				logger.warn("Run directory already exists - deleting");
				outName.delete();
			}
			PrintWriter outFile = new PrintWriter(outName);

			logger.info("Sorting the current run");
			Collections.sort(run);

			// Storing it
			for (DocumentTerm p : run) {
				outFile.println(p.getDocId() + "\t" + p.getTermId() + "\t" + p.getFrequency());
			}
			outFile.close();
		} catch (FileNotFoundException e) {
			logger.error("Cannot find run file within " + RUNSPREFIX, e);
		}
		run.clear();
	}

	/**
	 * Does needed math to return appropriate run number
	 * 
	 * @return current run number
	 */
	private long getRunNumber() {
		++runNumber;
		return runNumber - 1;
	}

	private int getNewId() {
		++wordId;
		return wordId - 1;
	}

	/**
	 * Creates a DocumentTerm pair from token and docid and adds it to the local
	 * vocabulary
	 * 
	 * @param token
	 *            Token to index
	 * @param docId
	 *            Document Id containing Token
	 * @param lVoc
	 *            local dictionary of Tokens->DocumentTerm
	 */
	private void index(String token, int docId, HashMap<Integer, DocumentTerm> lVoc) {
		int termId;
		if (!voc.containsKey(token)) {
			termId = getNewId();
			voc.put(token, termId);
		} else {
			termId = voc.get(token);
		}

		if (!lVoc.containsKey(termId)) {
			DocumentTerm p = new DocumentTerm(termId, docId, 1);
			lVoc.put(termId, p);
		} else {
			DocumentTerm p = lVoc.get(termId);
			p.incrementFrequency();
			// do we need this?
			lVoc.put(termId, p);
		}
	}

	private static final String CRL = "./data/crawl.zip";

	public static void main(String[] args) {
		File crawl = null;
		if (args.length == 1) {
			logger.info("Using user provided parameters");
			try {
				crawl = new File(args[0]);
			} catch (Exception e) {
				printUsage(e);
			}
		} else {
			logger.info("User did not provide 1 input argument; reverting to defaults...");
			crawl = new File(CRL);
		}

		Indexer idxr = new Indexer();
		idxr.indexZip(crawl);
	}

	private static void printUsage(Exception e) {
		logger.error("Error parsing user provided parameters: " + "Indexer <crawlerDataFolder>", e);
	}

}
